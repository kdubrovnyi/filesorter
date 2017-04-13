using System;
using System.IO;
using System.Linq;

namespace FileSorter
{
    public class FileGenerator
    {
        private static readonly Random Random = new Random();
        private const string Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        private readonly ILogger _logger;

        public FileGenerator(ILogger logger)
        {
            _logger = logger;
        }

        public void GenerateFile(string filename, long linesNumber = 5000000)
        {
            Console.WriteLine($"Generating {linesNumber} random lines to the file {filename}...");

            using (var writer = new StreamWriter(File.OpenWrite(filename)))
                for (var i = 0; i < linesNumber; i++)
                {
                    writer.WriteLine($"{RandomNumber()}. {RandomString()}");

                    if (i % 5000 == 0)
                        _logger.ReportProgress(i, linesNumber);
                }

            Console.WriteLine("File generated.");
        }

        public static int RandomNumber() => Random.Next();

        public static string RandomString()
        {
            return new string(Enumerable.Repeat(Chars, Random.Next(2, 256))
                .Select(s => s[Random.Next(s.Length)]).ToArray());
        }
    }
}