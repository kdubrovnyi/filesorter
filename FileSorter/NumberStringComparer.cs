using System;
using System.Collections;

namespace FileSorter
{
    public class NumberStringComparer : IComparer
    {
        private const string Separator = ". ";

        public int Compare(object x, object y)
        {
            var sX = x as string;
            var sY = y as string;

            if (sX == null || sY == null)
                throw new InvalidOperationException($"{nameof(NumberStringComparer)} must be used only for comparing string objects.");
            
            var xPart2 = SecondPart(sX);
            var yPart2 = SecondPart(sY);

            var part2Comparison = string.CompareOrdinal(xPart2, yPart2);

            if (part2Comparison != 0)
                return part2Comparison;

            var intXPart1 = FirstPart(sX);
            var intYPart1 = FirstPart(sY);

            return intXPart1 - intYPart1;
        }

        private static string SecondPart(string value)
        {
            var separatorIndex = value.IndexOf(Separator, StringComparison.Ordinal);
            if (separatorIndex < 0)
                throw new InvalidOperationException($"String must contain '{Separator}' separator.");

            var secondPart = value.Substring(separatorIndex + Separator.Length);

            if (string.IsNullOrWhiteSpace(secondPart))
                throw new InvalidOperationException("The second part of the string must not be empty or white space.");

            return secondPart;
        }

        private static int FirstPart(string value)
        {
            var firstPart = value.Substring(0, value.IndexOf(Separator, StringComparison.Ordinal));
            if (!int.TryParse(firstPart, out int firstPartInt))
                throw new InvalidOperationException("The first part of the string must be an integer number.");

            return firstPartInt;
        }
    }
}