namespace Mocha.Transport.Nats;

/// <summary>
/// Matches NATS subjects against filters containing the <c>*</c> and <c>&gt;</c> wildcards.
/// </summary>
public static class SubjectMatcher
{
    /// <summary>
    /// Determines whether a subject is captured by a filter.
    /// </summary>
    /// <param name="filter">The subject filter, which may contain wildcards.</param>
    /// <param name="subject">The concrete subject to test.</param>
    /// <returns><see langword="true"/> when the filter captures the subject.</returns>
    /// <remarks>
    /// <c>*</c> matches exactly one token and <c>&gt;</c> matches one or more trailing tokens.
    /// </remarks>
    public static bool Matches(string filter, string subject)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filter);
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);

        var filterTokens = filter.Split('.');
        var subjectTokens = subject.Split('.');

        for (var i = 0; i < filterTokens.Length; i++)
        {
            var filterToken = filterTokens[i];

            if (filterToken == ">")
            {
                return i < subjectTokens.Length;
            }

            if (i >= subjectTokens.Length)
            {
                return false;
            }

            if (filterToken != "*" && filterToken != subjectTokens[i])
            {
                return false;
            }
        }

        return filterTokens.Length == subjectTokens.Length;
    }
}
