using System;

namespace FileSorter
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args[0] == "-g" || args[0] == "-generate")
            {
                var generator = new FileGenerator(new ConsoleLogger());

                if (args.Length > 2 && long.TryParse(args[2], out long linesCount))
                    generator.GenerateFile(args[1], linesCount);
                else
                    generator.GenerateFile(args[1]);
            }
            else
            {
                if (args.Length < 2)
                    Console.WriteLine("Source and target file paths must be provided");
                else
                    new Sorter(new ConsoleLogger(), new NumberStringComparer()).Sort(args[0], args[1]);
            }
        }
    }
}