// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace VirtualClient.Actions
{
    using System;
    using System.Collections.Generic;
    using System.IO.Abstractions;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.CodeAnalysis;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;
    using Polly;
    using VirtualClient.Common;
    using VirtualClient.Common.Extensions;
    using VirtualClient.Common.Telemetry;
    using VirtualClient.Contracts;
    using VirtualClient.Contracts.Metadata;

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
                this.PackageName, this.Platform, this.CpuArchitecture, cancellationToken);

            DependencyPath javaPackage = await this.packageManager.GetPackageAsync(
                this.JdkPackageName, cancellationToken);

            this.PackageDirectory = workloadPackage.Path; // /home/azureuser/VirtualClient.13.9.3/content/linux-x64/packages/hadoop-3.3.5/linux-x64
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
            // this.ExecutableFilesAsync(telemetryContext, cancellationToken);

            await this.systemManagement.MakeFileExecutableAsync(this.ExecutablePath, this.Platform, cancellationToken);
            await this.systemManagement.MakeFileExecutableAsync(javaExecutablePath, this.Platform, cancellationToken);
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
            string path6 = this.PackageDirectory + "/sbin/stop-yarn.sh";

            await this.systemManagement.MakeFileExecutableAsync(path1, this.Platform, cancellationToken);
            await this.systemManagement.MakeFileExecutableAsync(path2, this.Platform, cancellationToken);
            await this.systemManagement.MakeFileExecutableAsync(path3, this.Platform, cancellationToken);
            await this.systemManagement.MakeFileExecutableAsync(path4, this.Platform, cancellationToken);
            await this.systemManagement.MakeFileExecutableAsync(path5, this.Platform, cancellationToken);
            await this.systemManagement.MakeFileExecutableAsync(path6, this.Platform, cancellationToken);

            await this.ExecuteCommandAsync("bash", $"-c \"echo y | ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa\"", this.PackageDirectory, telemetryContext, cancellationToken);
            await this.ExecuteCommandAsync("bash", $"-c \"cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys\"", this.PackageDirectory, telemetryContext, cancellationToken);
            await this.ExecuteCommandAsync("bash", $"-c \"chmod 0600 ~/.ssh/authorized_keys\"", this.PackageDirectory, telemetryContext, cancellationToken);
            await this.ExecuteCommandAsync("bash", $"-c \"bin/hdfs namenode -format\"", this.PackageDirectory, telemetryContext, cancellationToken);
            await this.ExecuteCommandAsync("bash", $"-c sbin/start-dfs.sh", this.PackageDirectory, telemetryContext, cancellationToken);
            await this.ExecuteCommandAsync("bash", $"-c \"bin/hdfs dfs -mkdir /user\"", this.PackageDirectory, telemetryContext, cancellationToken);
            await this.ExecuteCommandAsync("bash", $"-c \"bin/hdfs dfs -mkdir /user/azureuser\"", this.PackageDirectory, telemetryContext, cancellationToken);
            await this.ExecuteCommandAsync("bash", $"-c \"sbin/start-yarn.sh\"", this.PackageDirectory, telemetryContext, cancellationToken);
            // await this.ExecuteCommandAsync("bash", $"-c \"bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar teragen 100 /inp-{timestamp}\"", this.PackageDirectory, telemetryContext, cancellationToken);
            // await this.ExecuteCommandAsync("bash", $"-c \"bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar terasort /inp-{timestamp} /out-{timestamp}\"", this.PackageDirectory, telemetryContext, cancellationToken);

            // await this.ExecuteCommandAsync("bash", $"-c \"bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar teragen 100 /inp-{timestamp}\"", this.PackageDirectory, telemetryContext, cancellationToken);
            // await this.ExecuteCommandAsync("bash", $"-c \"bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar terasort 100 /inp-{timestamp} /out-{timestamp}\"", this.PackageDirectory, telemetryContext, cancellationToken);

            using (IProcessProxy process = await this.ExecuteCommandAsync("bash", $"-c \"bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar teragen 100 /inp-{timestamp}\"", this.PackageDirectory, telemetryContext, cancellationToken))
             {
               if (!cancellationToken.IsCancellationRequested)
               {
                   await this.LogProcessDetailsAsync(process, telemetryContext, "Hadoop Teragen", logToFile: true);

                   process.ThrowIfWorkloadFailed();
                   this.CaptureMetrics(process, telemetryContext, cancellationToken);
               }
             }

            await this.ExecuteCommandAsync("bash", $"-c \"bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar terasort /inp-{timestamp} /out-{timestamp}\"", this.PackageDirectory, telemetryContext, cancellationToken);

            await this.ExecuteCommandAsync("bash", $"-c \"sbin/stop-dfs.sh\"", this.PackageDirectory, telemetryContext, cancellationToken);
            await this.ExecuteCommandAsync("bash", $"-c \"sbin/stop-yarn.sh\"", this.PackageDirectory, telemetryContext, cancellationToken);

            return false;

            // state management
            //  - running the setup commands only once
            // list the commands and for loop
            // list make file executable
            // make the row number a variable
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

        // private async Task ExecutableFilesAsync(params string[] pathSegments)
        // {
        //     string javaExecutablePath = this.PlatformSpecifics.Combine(this.PackageDirectory, "bin", "java");

        // await this.systemManagement.MakeFileExecutableAsync(path1, this.Platform, cancellationToken)
        //    .ConfigureAwait(false);
        // }

        /*private Task ExecuteCommandAsync(string commandLine, string commandLineArguments, string workingDirectory, EventContext telemetryContext, CancellationToken cancellationToken)
        {
            EventContext relatedContext = telemetryContext.Clone();

            return this.RetryPolicy.ExecuteAsync(async () =>
            {
                using (IProcessProxy process = this.systemManagement.ProcessManager.CreateProcess(commandLine, commandLineArguments, workingDirectory))
                {
                    this.Logger.LogTraceMessage($"Executing process '{commandLine}' '{commandLineArguments}' at directory '{workingDirectory}'.");
                    this.CleanupTasks.Add(() => process.SafeKill());
                    this.LogProcessTrace(process);

                    await process.StartAndWaitAsync(cancellationToken);

                    if (!cancellationToken.IsCancellationRequested)
                    {
                        await this.LogProcessDetailsAsync(process, relatedContext, "HadoopExecutor");
                        process.ThrowIfErrored<DependencyException>(errorReason: ErrorReason.DependencyInstallationFailed);
                    }
                }
            });
        }*/

        /// <summary>
        /// Logs the Hadoop Terasort workload metrics.
        /// </summary>
        private void CaptureMetrics(IProcessProxy process, EventContext telemetryContext, CancellationToken cancellationToken)
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                this.MetadataContract.AddForScenario(
                    "Hadoop Teragen",
                    process.FullCommand(),
                    toolVersion: "3.3.5");

                this.MetadataContract.Apply(telemetryContext);

                HadoopMetricsParser parser = new HadoopMetricsParser(process.StandardError.ToString());
                IList<Metric> workloadMetrics = parser.Parse();

                this.Logger.LogMetrics(
                    "Hadoop Teragen",
                    this.Scenario,
                    process.StartTime,
                    process.ExitTime,
                    workloadMetrics,
                    null,
                    process.FullCommand(),
                    this.Tags,
                    telemetryContext);
            }
        }
    }
}