using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace FabricTableService.Utilities
{
    using System.Linq;
    using System.Text;

    public static class ExceptionLogging
    {
        /// <summary>
        /// Configures exception logging such that first-chance and unhandled exceptions are logged to the trace listener.
        /// </summary>
        /// <param name="firstChance">
        /// Whether or not to log first-chance exceptions.
        /// </param>
        public static void Setup(bool firstChance = true)
        {
            if (firstChance)
            {
                AppDomain.CurrentDomain.FirstChanceException += (sender, eventArgs) =>
                {
                    var exception = eventArgs.Exception;
                    if (exception == null)
                    {
                        return;
                    }

                    var message = $"AppDomain.FirstChanceException: {exception.ToDetailedString()}";
                    Trace.TraceWarning(message);
                };
            }

            AppDomain.CurrentDomain.UnhandledException += (sender, eventArgs) =>
            {
                var exception = eventArgs.ExceptionObject as Exception;
                if (exception == null)
                {
                    return;
                }

                var message = $"AppDomain.UnhandledException: {exception.ToDetailedString()}";
                Trace.TraceError(message);
            };

            TaskScheduler.UnobservedTaskException += (sender, eventArgs) =>
            {
                if (eventArgs.Exception == null)
                {
                    return;
                }

                var message = $"TaskScheduler.UnobservedTaskException: {eventArgs.Exception.ToDetailedString()}";
                Trace.TraceError(message);
            };
        }
        /// <summary>
        /// Returns a detailed string representation of this instance.
        /// </summary>
        /// <param name="exception">
        /// The exception.
        /// </param>
        /// <returns>
        /// A detailed string representation of this instance.
        /// </returns>
        /// <remarks>
        /// Returns <see cref="string.Empty"/> if <paramref name="exception"/> is null.
        /// </remarks>
        public static string ToDetailedString(this Exception exception)
        {
            if (exception == null)
            {
                return string.Empty;
            }

            var result = new StringBuilder();
            result.AppendFormat("{0} - {1}\n", exception.GetType(), exception.Message);

            if (exception.StackTrace != null)
            {
                result.AppendFormat("Stack: {0}\n", exception.StackTrace);
            }

            if (!string.IsNullOrWhiteSpace(exception.Source))
            {
                result.AppendFormat("Source: {0}\n", exception.Source);
            }

            var ag = exception as AggregateException;
            if (ag != null)
            {
                result.AppendLine(string.Join("\n\n", ag.Flatten().InnerExceptions.Select(ToDetailedString)));
            }
            else if (exception.InnerException != null)
            {
                result.AppendLine(exception.InnerException.ToDetailedString());
            }

            return result.ToString();
        }
    }
}