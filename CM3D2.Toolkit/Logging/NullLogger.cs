// --------------------------------------------------
// CM3D2.Toolkit - NullLogger.cs
// --------------------------------------------------

namespace CM3D2.Toolkit.Guest4168Branch.Logging
{
    /// <summary>
    ///     Null Logger
    /// </summary>
    public sealed class NullLogger : ILogger
    {
        /// <summary>
        ///     Null Logger Instance
        /// </summary>
        public static NullLogger Instance = new NullLogger();

        private NullLogger() {}

        /// <inheritdoc />
        public void Debug(string message, params object[] args) {}

        /// <inheritdoc />
        public void Error(string message, params object[] args) {}

        /// <inheritdoc />
        public void Info(string message, params object[] args) {}

        /// <inheritdoc />
        public void Trace(string message, params object[] args) {}

        /// <inheritdoc />
        public void Warn(string message, params object[] args) {}

        /// <inheritdoc />
        public void Fatal(string message, params object[] args) {}

        /// <inheritdoc />
        public void GuestLevel1(string message, params object[] args) { }

        /// <inheritdoc />
        public void GuestLevel2(string message, params object[] args) { }

        /// <inheritdoc />
        public void GuestLevel3(string message, params object[] args) { }

        /// <inheritdoc />
        public void GuestLevel4(string message, params object[] args) { }

        /// <inheritdoc />
        public void GuestLevel5(string message, params object[] args) { }

        public string Name => nameof(NullLogger);
    }
}
