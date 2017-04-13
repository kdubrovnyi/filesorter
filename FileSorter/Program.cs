using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Linq;

namespace FileSorter
{
    class Program
    {
        private static readonly Random Random = new Random();

        static void Main(string[] args)
        {
            if (args[0] == "-g" || args[0] == "-generate")
            {
                int linesCount;
                if (args.Length <= 2 || !int.TryParse(args[2], out linesCount))
                    linesCount = 5000000;

                Console.WriteLine($"Generating {linesCount} random lines to the file {args[1]}...");

                using (var writer = new StreamWriter(File.OpenWrite(args[1])))
                    for (int i = 0; i < linesCount; i++)
                    {
                        if (i % 5000 == 0)
                            Console.Write("{0:f2}%   \r", 100.0 * i / linesCount);
                        writer.WriteLine($"{Random.Next()}. {RandomString(Random.Next(2, 256))}");
                    }

                Console.WriteLine("File generated.");
            }
            else
            {
                if (args.Length < 2)
                    Console.WriteLine("Source and target file paths must be provided.");
                else
                {
                    var source = args[0];
                    var target = args[1];
                    
                    var chunks = SplitToChunks(source);
                    DisplayMemoryUsage();
                    var sortedChunks = SortTheChunks(chunks);
                    DisplayMemoryUsage();
                    MergeTheChunks(sortedChunks, target);
                    DisplayMemoryUsage();

                    // This does a external merge sort on a big file
                    // http://en.wikipedia.org/wiki/External_sorting
                    // The idea is to keep the memory usage below 50megs.
                }
            }
        }

        static List<string> SplitToChunks(string file, long mazSize = 50 * 1024 * 1024)
        {
            var chunkFiles = new List<string>();

            W("Splitting");
            var splitNum = 1;

            var chunkFile = $"{file}{splitNum}";
            chunkFiles.Add(chunkFile);

            var sw = new StreamWriter(chunkFile);
            long readLine = 0;
            using (var sr = new StreamReader(file))
            {
                while (sr.Peek() >= 0)
                {
                    if (++readLine % 5000 == 0)
                        Console.Write("{0:f2}%   \r", 100.0 * sr.BaseStream.Position / sr.BaseStream.Length);

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
            sw.Close();
            W("Splitting complete");
            return chunkFiles;
        }

        static List<string> SortTheChunks(List<string> chunkFiles)
        {
            W("Sorting chunks");
            var sortedChunkFiles = chunkFiles.AsParallel().Select(SortChunk).ToList();
            W("Sorting chunks completed");
            return sortedChunkFiles;
        }

        private static string SortChunk(string path)
        {
            Console.Write("{0}     \r", path);
            string[] contents = File.ReadAllLines(path);
            Array.Sort(contents, new NumberStringComparer());
            string newpath = $"{path}sorted";
            File.WriteAllLines(newpath, contents);
            File.Delete(path);
            return newpath;
        }

        static void MergeTheChunks(List<string> sortedChunkFiles, string targetFile)
        {
            W("Merging");

            int chunksNumber = sortedChunkFiles.Count; // Number of chunks
            int recordSize = 100; // estimated record size
            int records = 10000000; // estimated total # records
            int maxUsage = 500000000; // max memory usage
            int bufferSize = maxUsage / chunksNumber; // size in bytes of each buffer
            double recordOverhead = 7.5; // The overhead of using Queue<>
            int bufferLen = (int)(bufferSize / recordSize / recordOverhead); // number of records in each buffer

            // Open the files
            var readers = new StreamReader[chunksNumber];
            for (var i = 0; i < chunksNumber; i++)
                readers[i] = new StreamReader(sortedChunkFiles[i]);

            // Make the queues
            Queue<string>[] queues = new Queue<string>[chunksNumber];
            for (int i = 0; i < chunksNumber; i++)
                queues[i] = new Queue<string>(bufferLen);

            // Load the queues
            W("Priming the queues");
            for (int i = 0; i < chunksNumber; i++)
                LoadQueue(queues[i], readers[i], bufferLen);
            W("Priming the queues complete");

            // Merge!
            StreamWriter sw = new StreamWriter(targetFile);
            bool done = false;
            int lowest_index, j, progress = 0;
            string lowest_value;
            while (!done)
            {
                // Report the progress
                if (++progress % 5000 == 0)
                    Console.Write("{0:f2}%   \r",
                      100.0 * progress / records);

                // Find the chunk with the lowest value
                lowest_index = -1;
                lowest_value = "";
                for (j = 0; j < chunksNumber; j++)
                {
                    if (queues[j] != null)
                    {
                        if (lowest_index < 0 ||  new NumberStringComparer().Compare(queues[j].Peek(), lowest_value) < 0)
                        {
                            lowest_index = j;
                            lowest_value = queues[j].Peek();
                        }
                    }
                }

                // Was nothing found in any queue? We must be done then.
                if (lowest_index == -1) { done = true; break; }

                // Output it
                sw.WriteLine(lowest_value);

                // Remove from queue
                queues[lowest_index].Dequeue();
                // Have we emptied the queue? Top it up
                if (queues[lowest_index].Count == 0)
                {
                    LoadQueue(queues[lowest_index], readers[lowest_index], bufferLen);
                    // Was there nothing left to read?
                    if (queues[lowest_index].Count == 0)
                    {
                        queues[lowest_index] = null;
                    }
                }
            }
            sw.Close();

            // Close and delete the files
            for (int i = 0; i < chunksNumber; i++)
            {
                readers[i].Close();
                File.Delete(sortedChunkFiles[i]);
            }

            W("Merging complete");
        }

        static void LoadQueue(Queue<string> queue, StreamReader file, int records)
        {
            for (int i = 0; i < records; i++)
            {
                if (file.Peek() < 0) break;
                queue.Enqueue(file.ReadLine());
            }
        }

        static void W(string s)
        {
            Console.WriteLine("{0}: {1}", DateTime.Now.ToLongTimeString(), s);
        }

        static void DisplayMemoryUsage()
        {
            W(String.Format("{0} MB peak working set | {1} MB private bytes",
              Process.GetCurrentProcess().PeakWorkingSet64 / 1024 / 1024,
              Process.GetCurrentProcess().PrivateMemorySize64 / 1024 / 1024
              ));
        }

        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[Random.Next(s.Length)]).ToArray());
        }
    }
}