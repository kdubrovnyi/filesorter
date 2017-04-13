using System;

namespace FileSorter
{
    public class ConsoleLogger : ILogger
    {
        public void Log(string message) => Console.WriteLine($"{DateTime.Now.ToLongTimeString()}: {message}");
        public void ReportProgress(long progress, long total) => ReportProgress($"{100.0 * progress / total:f2}%   \r");
        public void ReportProgress(string message) => Console.Write(message);
    }
}