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

            int xPart1;
            string xPart2;
            Split(sX, out xPart1, out xPart2);
            int yPart1;
            string yPart2;
            Split(sY, out yPart1, out yPart2);

            var part2Comparison = string.CompareOrdinal(xPart2, yPart2);

            if (part2Comparison != 0)
                return part2Comparison;

            return xPart1 - yPart1;
        }

        private static void Split(string value, out int part1, out string part2)
        {
            var parts = value.Split(new[] {Separator}, StringSplitOptions.None);
            if (parts.Length != 2)
                throw new InvalidOperationException($"The compared string must contain 2 parts delimited by {Separator}");

            if (!int.TryParse(parts[0], out part1))
                throw new InvalidOperationException("The first part of the string must be an integer number");

            if (string.IsNullOrWhiteSpace(parts[1]))
                throw new InvalidOperationException("The second part of the string must not be empty or white space");

            part2 = parts[1];
        }
    }
}