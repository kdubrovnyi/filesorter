namespace FileSorter
{
    public interface ILogger
    {
        void Log(string message);
        void ReportProgress(long progress, long total);
        void ReportProgress(string message);
    }
}