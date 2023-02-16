namespace CM3D2.Toolkit.Guest4168Branch.Logging
{
    /// <summary>
    ///     Simple Logging Abstraction Layer
    /// </summary>
    public interface ILogger
    {
        /// <summary>
        ///     Debug-Level Log Message
        /// </summary>
        /// <param name="message">Message</param>
        /// <param name="args">Parameters</param>
        void Debug(string message, params object[] args);

        /// <summary>
        ///     Error-Level Log Message
        /// </summary>
        /// <param name="message">Message</param>
        /// <param name="args">Parameters</param>
        void Error(string message, params object[] args);

        /// <summary>
        ///     Info-Level Log Message
        /// </summary>
        /// <param name="message">Message</param>
        /// <param name="args">Parameters</param>
        void Info(string message, params object[] args);

        /// <summary>
        ///     Trace-Level Log Message
        /// </summary>
        /// <param name="message">Message</param>
        /// <param name="args">Parameters</param>
        void Trace(string message, params object[] args);

        /// <summary>
        ///     Warn-Level Log Message
        /// </summary>
        /// <param name="message">Message</param>
        /// <param name="args">Parameters</param>
        void Warn(string message, params object[] args);

        /// <summary>
        ///     Fatal-Level Log Message
        /// </summary>
        /// <param name="message">Message</param>
        /// <param name="args">Parameters</param>
        void Fatal(string message, params object[] args);

        void GuestLevel1(string message, params object[] args);
        void GuestLevel2(string message, params object[] args);
        void GuestLevel3(string message, params object[] args);
        void GuestLevel4(string message, params object[] args);
        void GuestLevel5(string message, params object[] args);

        /// <summary>
        ///     Logger Name
        /// </summary>
        string Name { get; }
    }
}