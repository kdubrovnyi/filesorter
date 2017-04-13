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

            var chunksNumber = sortedChunkFiles.Count; // Number of chunks
            var bufferSize = maxMemory / chunksNumber; // size in bytes of each buffer
            var recordOverhead = 7.5; // The overhead of using Queue<>
            var bufferLen = (int)(bufferSize / estimatedRecordSize / recordOverhead); // number of records in each buffer

            // Open the files
            var readers = new StreamReader[chunksNumber];
            for (var i = 0; i < chunksNumber; i++)
                readers[i] = new StreamReader(sortedChunkFiles[i]);

            // Make the queues
            Queue<string>[] queues = new Queue<string>[chunksNumber];
            for (int i = 0; i < chunksNumber; i++)
                queues[i] = new Queue<string>(bufferLen);

            // Load the queues
            _logger.Log("Priming the queues");
            for (int i = 0; i < chunksNumber; i++)
                LoadQueue(queues[i], readers[i], bufferLen);
            _logger.Log("Priming the queues complete");

            // Merge!
            var sw = new StreamWriter(targetFile);
            int j, progress = 0;
            while (true)
            {
                // Report the progress
                if (++progress % 5000 == 0)
                    _logger.ReportProgress(progress, estimatedRecordsNumber);

                // Find the chunk with the lowest value
                var lowestIndex = -1;
                var lowestValue = "";
                for (j = 0; j < chunksNumber; j++)
                {
                    if (queues[j] != null)
                    {
                        if (lowestIndex < 0 || Compare(queues[j].Peek(), lowestValue) < 0)
                        {
                            lowestIndex = j;
                            lowestValue = queues[j].Peek();
                        }
                    }
                }

                // Was nothing found in any queue? We must be done then.
                if (lowestIndex == -1) {
                    break; }

                // Output it
                sw.WriteLine(lowestValue);

                // Remove from queue
                queues[lowestIndex].Dequeue();
                // Have we emptied the queue? Top it up
                if (queues[lowestIndex].Count == 0)
                {
                    LoadQueue(queues[lowestIndex], readers[lowestIndex], bufferLen);
                    // Was there nothing left to read?
                    if (queues[lowestIndex].Count == 0)
                        queues[lowestIndex] = null;
                }
            }
            sw.Close();

            // Close and delete the files
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