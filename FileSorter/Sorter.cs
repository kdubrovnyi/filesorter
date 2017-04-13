using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace FileSorter
{
    public class Sorter
    {
        private readonly ILogger _logger;
        private readonly IComparer _comparer;

        public Sorter(ILogger logger, IComparer comparer = null)
        {
            _logger = logger;
            _comparer = comparer;
        }

        public void Sort(string sourceFile, string targetFile)
        {
            // http://en.wikipedia.org/wiki/External_sorting
            var chunks = SplitToChunks(sourceFile);
            DisplayMemoryUsage();
            var sortedChunks = SortChunks(chunks);
            DisplayMemoryUsage();
            MergeTheChunks(sortedChunks, targetFile);
            DisplayMemoryUsage();
        }

        private List<string> SplitToChunks(string file, long mazSize = 50 * 1024 * 1024)
        {
            var chunkFiles = new List<string>();

            _logger.Log("Splitting");
            var splitNum = 1;

            var chunkFile = $"{file}{splitNum}";
            chunkFiles.Add(chunkFile);

            var sw = new StreamWriter(chunkFile);
            try
            {
                long readLine = 0;
                using (var sr = new StreamReader(file))
                {
                    while (sr.Peek() >= 0)
                    {
                        if (++readLine % 5000 == 0)
                            _logger.ReportProgress(sr.BaseStream.Position, sr.BaseStream.Length);

                        sw.WriteLine(sr.ReadLine());

                        if (sw.BaseStream.Length > mazSize && sr.Peek() >= 0)
                        {
                            sw.Close();
                            splitNum++;
                            chunkFile = $"{file}{splitNum}";
                            chunkFiles.Add(chunkFile);

                            sw = new StreamWriter(chunkFile);
                        }
                    }
                }
            }
            finally
            {
                sw.Close();
            }

            _logger.Log("Splitting complete");
            return chunkFiles;
        }

        private List<string> SortChunks(List<string> chunkFiles)
        {
            _logger.Log("Sorting chunks");
            var sortedChunkFiles = chunkFiles.AsParallel().Select(SortChunk).ToList();
            _logger.Log("Sorting chunks completed");
            return sortedChunkFiles;
        }

        private string SortChunk(string path)
        {
            _logger.ReportProgress($"{path}     \r");

            var contents = File.ReadAllLines(path);

            if (_comparer != null)
                Array.Sort(contents, _comparer);
            else
                Array.Sort(contents);

            var newpath = $"{path}sorted";
            File.WriteAllLines(newpath, contents);
            File.Delete(path);
            return newpath;
        }

        private void MergeTheChunks(
            List<string> sortedChunkFiles,
            string targetFile,
            long maxMemory = 500000000,
            long estimatedRecordsNumber = 10000000,
            int estimatedRecordSize = 100)
        {
            _logger.Log("Merging");

            var chunksNumber = sortedChunkFiles.Count;
            var bufferSize = maxMemory / chunksNumber;
            var recordOverhead = 7.5; // The overhead of using Queue<>
            var bufferLen = (int)(bufferSize / estimatedRecordSize / recordOverhead); // number of records in each buffer

            var readers = new StreamReader[chunksNumber];
            for (var i = 0; i < chunksNumber; i++)
                readers[i] = new StreamReader(sortedChunkFiles[i]);

            var queues = new Queue<string>[chunksNumber];
            for (var i = 0; i < chunksNumber; i++)
                queues[i] = new Queue<string>(bufferLen);

            _logger.Log("Loading queues");
            for (var i = 0; i < chunksNumber; i++)
                LoadQueue(queues[i], readers[i], bufferLen);
            _logger.Log("Loading queues complete");

            var sw = new StreamWriter(targetFile);
            var progress = 0;
            while (true)
            {
                if (++progress % 5000 == 0)
                    _logger.ReportProgress(progress, estimatedRecordsNumber);

                // Find the chunk with the lowest value
                var lowestChunkIndex = -1;
                var lowestValue = "";
                int i;
                for (i = 0; i < chunksNumber; i++)
                {
                    if (queues[i] != null)
                    {
                        if (lowestChunkIndex < 0 || Compare(queues[i].Peek(), lowestValue) < 0)
                        {
                            lowestChunkIndex = i;
                            lowestValue = queues[i].Peek();
                        }
                    }
                }

                if (lowestChunkIndex == -1)
                    break;

                sw.WriteLine(lowestValue);

                queues[lowestChunkIndex].Dequeue();
                if (queues[lowestChunkIndex].Count == 0)
                {
                    LoadQueue(queues[lowestChunkIndex], readers[lowestChunkIndex], bufferLen);
                    if (queues[lowestChunkIndex].Count == 0)
                        queues[lowestChunkIndex] = null;
                }
            }
            sw.Close();

            for (var i = 0; i < chunksNumber; i++)
            {
                readers[i].Close();
                File.Delete(sortedChunkFiles[i]);
            }

            _logger.Log("Merging complete");
        }

        private static void LoadQueue(Queue<string> queue, StreamReader file, int records)
        {
            for (var i = 0; i < records; i++)
            {
                if (file.Peek() < 0) break;
                queue.Enqueue(file.ReadLine());
            }
        }

        private void DisplayMemoryUsage()
        {
            var peakWorkingSet = Process.GetCurrentProcess().PeakWorkingSet64 / 1024 / 1024;
            var privateMemorySize = Process.GetCurrentProcess().PrivateMemorySize64 / 1024 / 1024;
            _logger.Log($"{peakWorkingSet} MB peak working set | {privateMemorySize} MB private bytes");
        }

        private int Compare(string x, string y) => _comparer?.Compare(x, y) ?? string.CompareOrdinal(x, y);
    }
}