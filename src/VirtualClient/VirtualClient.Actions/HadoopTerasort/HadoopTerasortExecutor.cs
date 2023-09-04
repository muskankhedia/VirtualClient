// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace VirtualClient.Actions
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.IO.Abstractions;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.CodeAnalysis;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;
    using Microsoft.VisualBasic;
    using Polly;
    using VirtualClient.Common;
    using VirtualClient.Common.Extensions;
    using VirtualClient.Common.Telemetry;
    using VirtualClient.Contracts;
    using static Azure.Core.HttpHeader;

    /// <summary>
    /// The Hadoop Terasort workload executor
    /// </summary>
    public class HadoopTerasortExecutor : VirtualClientComponent
    {
        private IFileSystem fileSystem;
        private IPackageManager packageManager;
        private ISystemManagement systemManagement;

        /// <summary>
        /// Constructor for <see cref="HadoopTerasortExecutor"/>
        /// </summary>
        /// <param name="dependencies">Provides required dependencies to the component.</param>
        /// <param name="parameters">Parameters defined in the profile or supplied on the command line.</param>
        public HadoopTerasortExecutor(IServiceCollection dependencies, IDictionary<string, IConvertible> parameters)
             : base(dependencies, parameters)
        {
            this.RetryPolicy = Policy.Handle<Exception>().WaitAndRetryAsync(5, (retries) => TimeSpan.FromSeconds(retries + 1));
            this.systemManagement = this.Dependencies.GetService<ISystemManagement>();
            this.packageManager = this.systemManagement.PackageManager;
            this.fileSystem = this.systemManagement.FileSystem;
        }

        /// <summary>
        /// A policy that defines how the component will retry when
        /// it experiences transient issues.
        /// </summary>
        public IAsyncPolicy RetryPolicy { get; set; }

        /// <summary>
        /// Java Development Kit package name.
        /// </summary>
        public string JdkPackageName
        {
            get
            {
                return this.Parameters.GetValue<string>(nameof(HadoopTerasortExecutor.JdkPackageName));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string CommandLine
        {
            get
            {
                return this.Parameters.GetValue<string>(nameof(HadoopTerasortExecutor.CommandLine));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string CommandArguments
        {
            get
            {
                return this.Parameters.GetValue<string>(nameof(HadoopTerasortExecutor.CommandArguments));
            }
        }

        /// <summary>
        /// The path to the Hadoop package.
        /// </summary>
        private string PackageDirectory { get; set; }

        /// <summary>
        /// 
        /// </summary>
        private string JavaPackageDirectory { get; set; }

        /// <summary>
        /// The path to the Hadoop executable file.
        /// </summary>
        private string ExecutablePath { get; set; }

        /// <summary>
        /// Initializes the environment for execution of the Hadoop Terasort workload.
        /// </summary>
        protected override async Task InitializeAsync(EventContext telemetryContext, CancellationToken cancellationToken)
        {
            DependencyPath workloadPackage = await this.packageManager.GetPlatformSpecificPackageAsync(
                this.PackageName, this.Platform, this.CpuArchitecture, cancellationToken)
                .ConfigureAwait(false);

            DependencyPath javaPackage = await this.packageManager.GetPackageAsync(
                this.JdkPackageName, cancellationToken)
                .ConfigureAwait(false);

            this.PackageDirectory = workloadPackage.Path;
            this.JavaPackageDirectory = javaPackage.Path;

            string javaExecutablePath = this.PlatformSpecifics.Combine(this.JavaPackageDirectory, "bin", "java");

            switch (this.Platform)
            {
                case PlatformID.Unix:
                    this.ExecutablePath = this.PlatformSpecifics.Combine(this.PackageDirectory, "bin", "hadoop");
                    break;

                default:
                    throw new WorkloadException(
                        $"The Hadoop workload is not supported on the current platform/architecture " +
                        $"{PlatformSpecifics.GetPlatformArchitectureName(this.Platform, this.CpuArchitecture)}." +
                        ErrorReason.PlatformNotSupported);
            }

            this.SetEnvironmentVariable(EnvironmentVariable.JAVA_HOME, this.JavaPackageDirectory, EnvironmentVariableTarget.Process);

            this.ConfigurationFilesAsync(telemetryContext, cancellationToken);
            this.ExecutableFilesAsync(telemetryContext, cancellationToken);

            await this.systemManagement.MakeFileExecutableAsync(this.ExecutablePath, this.Platform, cancellationToken)
                .ConfigureAwait(false);

            await this.systemManagement.MakeFileExecutableAsync(javaExecutablePath, this.Platform, cancellationToken)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Executes the Hadoop Terasort workload.
        /// </summary>
        protected override async Task<bool> ExecuteAsync(EventContext telemetryContext, CancellationToken cancellationToken)
        {
            string timestamp = DateTime.Now.ToString("ddMMyyHHmmss");

            string path1 = this.PackageDirectory + "/bin/hdfs";
            string path2 = this.PackageDirectory + "/sbin/start-dfs.sh";
            string path3 = this.PackageDirectory + "/sbin/stop-dfs.sh";
            string path4 = this.PackageDirectory + "/bin/yarn";
            string path5 = this.PackageDirectory + "/sbin/start-yarn.sh";

            await this.systemManagement.MakeFileExecutableAsync(path1, this.Platform, cancellationToken)
                .ConfigureAwait(false);
            await this.systemManagement.MakeFileExecutableAsync(path2, this.Platform, cancellationToken)
                .ConfigureAwait(false);
            await this.systemManagement.MakeFileExecutableAsync(path3, this.Platform, cancellationToken)
                .ConfigureAwait(false);
            await this.systemManagement.MakeFileExecutableAsync(path4, this.Platform, cancellationToken)
                .ConfigureAwait(false);
            await this.systemManagement.MakeFileExecutableAsync(path5, this.Platform, cancellationToken)
                .ConfigureAwait(false);

            List<string> result = this.CommandArguments.Split(',').ToList();

            foreach (string r in result)
            {
                await this.ExecuteCommandAsync(this.CommandLine, r, this.PackageDirectory, telemetryContext, cancellationToken)
                    .ConfigureAwait(false);
            }

           /* await this.ExecuteCommandAsync("bash", "-c \"ssh-keygen-t rsa -P '' -f ~/.ssh/id_rsa\"", this.PackageDirectory, telemetryContext, cancellationToken)
                    .ConfigureAwait(false);

            await this.ExecuteCommandAsync("bash", "-c 'cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys'", this.PackageDirectory, telemetryContext, cancellationToken)
                    .ConfigureAwait(false);*/

            /*await this.ExecuteCommandAsync("sudo", "apt-get install ssh", Environment.CurrentDirectory, telemetryContext, cancellationToken)
                    .ConfigureAwait(false);

            

            await this.ExecuteCommandAsync("chmod", "0600 ~/.ssh/authorized_keys", this.PackageDirectory, telemetryContext, cancellationToken)
                    .ConfigureAwait(false);*/

            // await this.ExecuteCommandAsync("ssh", "localhost", this.PackageDirectory, telemetryContext, cancellationToken)
            // .ConfigureAwait(false);

            /*string path6 = this.PackageDirectory + "/bin";
            await this.ExecuteCommandAsync("bash", "-c 'bin/hdfs namenode -format'", this.PackageDirectory, telemetryContext, cancellationToken)
                    .ConfigureAwait(false);

            await this.ExecuteCommandAsync("sbin/start-dfs.sh", null, this.PackageDirectory, telemetryContext, cancellationToken)
                    .ConfigureAwait(false);

            await this.ExecuteCommandAsync("/usr/bin/env", "./hdfs dfs -mkdir /user/azureuser", this.PackageDirectory, telemetryContext, cancellationToken)
                    .ConfigureAwait(false);

            await this.ExecuteCommandAsync("sbin/start-yarn.sh", null, this.PackageDirectory, telemetryContext, cancellationToken)
                    .ConfigureAwait(false);

            await this.ExecuteCommandAsync("/usr/bin/env", "./hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar teragen 100 /inp-{timestamp}", path6, telemetryContext, cancellationToken)
                    .ConfigureAwait(false);*/

            /*foreach (string command in commands1)
            {
                await this.ExecuteCommandAsync(string.Empty, command, Environment.CurrentDirectory, telemetryContext, cancellationToken)
                    .ConfigureAwait(false);
            }*/

            /*foreach (string command in commands)
            {
                *//*using (IProcessProxy process = await this.ExecuteCommandAsync(command, this.PackageDirectory, telemetryContext, cancellationToken)
                   .ConfigureAwait(false))
                {
                    if (!cancellationToken.IsCancellationRequested)
                    {
                        // await this.LogProcessDetailsAsync(process, telemetryContext, "7Zip", logToFile: true);
                        ConsoleLogger.Default.LogInformation($"command: {command}");
                        process.ThrowIfWorkloadFailed();
                        // this.CaptureMetrics(process, telemetryContext, commandLineArguments);
                    }
                }*//*
                await this.ExecuteCommandAsync(string.Empty, command, this.PackageDirectory, telemetryContext, cancellationToken)
                    .ConfigureAwait(false);
            }*/

            /*ConsoleLogger.Default.LogInformation($"Commands assigned");

            foreach (string command in commands)
            {
                ConsoleLogger.Default.LogInformation($"Executing Command: {command}");

                await this.ExecuteCommandAsync(command, null, this.PackageDirectory, telemetryContext, cancellationToken, true)
                    .ConfigureAwait(false);
            }

            // Start the server
            this.ExecuteCommandAsync("sbin/start-yarn.sh", null, this.PackageDirectory, telemetryContext, cancellationToken, true)
                    .ConfigureAwait(false);*/
            // Run mapreduce job based on user input

            // Mapreduce commands - teragen
            // string teragenCommand = "hadoop jar hadoop-*examples*.jar teragen 10000 /out";

            // Collect metrics
            // Stop the server

            return false;
        }

        private void ConfigurationFilesAsync(EventContext telemetryContext, CancellationToken cancellationToken)
        {
            ConsoleLogger.Default.LogInformation($"ConfigurationFilesAsync");

            IDictionary<string, string> coreSite = new Dictionary<string, string>
            {
                { "fs.defaultFS", "hdfs://localhost:9000" }
            };
            this.CreateHTMLValue("core-site.xml", coreSite, cancellationToken);

            IDictionary<string, string> hdfsSite = new Dictionary<string, string>
            {
                { "dfs.replication", "1" }
            };
            this.CreateHTMLValue("hdfs-site.xml", hdfsSite, cancellationToken);

            IDictionary<string, string> mapredSite = new Dictionary<string, string>
            {
                { "mapreduce.framework.name", "yarn" },
                { "mapreduce.application.classpath", "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*" }
            };
            this.CreateHTMLValue("mapred-site.xml", mapredSite, cancellationToken);

            IDictionary<string, string> yarnSite = new Dictionary<string, string>
            {
                { "yarn.nodemanager.aux-services", "mapreduce_shuffle" },
                { "yarn.nodemanager.env-whitelist", "JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME" }
            };
            this.CreateHTMLValue("yarn-site.xml", yarnSite, cancellationToken);

            string makeFilePath = this.PlatformSpecifics.Combine(this.PackageDirectory, "etc", "hadoop");
            string hadoopEnvFilePath = this.PlatformSpecifics.Combine(makeFilePath, "hadoop-env.sh");

            this.fileSystem.File.ReplaceInFileAsync(
                        hadoopEnvFilePath, @"# export JAVA_HOME=", $"export JAVA_HOME={this.JavaPackageDirectory}", cancellationToken);
        }

        private void CreateHTMLValue(string fileName, IDictionary<string, string> value, CancellationToken cancellationToken)
        {
            string makeFilePath = this.PlatformSpecifics.Combine(this.PackageDirectory, "etc", "hadoop");
            ConsoleLogger.Default.LogInformation($"makeFilePath: {makeFilePath}");

            string filePath = this.PlatformSpecifics.Combine(makeFilePath, fileName);
            ConsoleLogger.Default.LogInformation($"filePath: {filePath}");

            string replaceStatement = @"<configuration>([\s\S]*?)<\/configuration>";
            string replacedStatement = "<configuration>";

            for (int i = 0; i < value.Count; i++)
            {
                string property = $"<property><name>{value.ElementAt(i).Key}</name><value>{value.ElementAt(i).Value}</value></property>";
                replacedStatement += property;
            }

            replacedStatement += "</configuration>";
            ConsoleLogger.Default.LogInformation($"replacedStatement: {replacedStatement}");

            this.fileSystem.File.ReplaceInFileAsync(
                    filePath, replaceStatement, replacedStatement, cancellationToken);
        }

        private void ExecutableFilesAsync(params string[] pathSegments)
        {
            string javaExecutablePath = this.PlatformSpecifics.Combine(this.PackageDirectory, "bin", "java");

            await this.systemManagement.MakeFileExecutableAsync(path1, this.Platform, cancellationToken)
                .ConfigureAwait(false);
        }

        private Task ExecuteCommandAsync(string commandLine, string arguments, string workingDirectory, EventContext telemetryContext, CancellationToken cancellationToken)
        {
            EventContext relatedContext = telemetryContext.Clone();

            return this.RetryPolicy.ExecuteAsync(async () =>
            {
                using (IProcessProxy process = this.systemManagement.ProcessManager.CreateProcess(commandLine, arguments, workingDirectory))
                {
                    ConsoleLogger.Default.LogInformation($"command: {arguments},,,, workingDirectory: {workingDirectory}");
                    this.CleanupTasks.Add(() => process.SafeKill());
                    this.LogProcessTrace(process);

                    await process.StartAndWaitAsync(cancellationToken)
                        .ConfigureAwait(false);

                    if (!cancellationToken.IsCancellationRequested)
                    {
                        await this.LogProcessDetailsAsync(process, relatedContext, "HadoopExecutor")
                            .ConfigureAwait(false);

                        process.ThrowIfErrored<DependencyException>(errorReason: ErrorReason.DependencyInstallationFailed);
                    }
                }
            });
        }
    }
}
