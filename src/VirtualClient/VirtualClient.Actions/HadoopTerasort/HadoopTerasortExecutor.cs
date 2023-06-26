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
    using VirtualClient.Common;
    using VirtualClient.Common.Extensions;
    using VirtualClient.Common.Telemetry;
    using VirtualClient.Contracts;

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
            this.systemManagement = this.Dependencies.GetService<ISystemManagement>();
            this.packageManager = this.systemManagement.PackageManager;
            this.fileSystem = this.systemManagement.FileSystem;
        }

        /// <summary>
        /// Java Development Kit package name.
        /// </summary>
        public string JdkPackageName
        {
            get
            {
                return this.Parameters.GetValue<string>(nameof(SpecJbbExecutor.JdkPackageName));
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

            ConsoleLogger.Default.LogInformation($"workloadPackage: {workloadPackage}");
            ConsoleLogger.Default.LogInformation($"PackageDirectory: {this.PackageDirectory}"); // /home/azureuser/VirtualClient.13.6.1/content/linux-x64/packages/hadoop-3.3.5/linux-x64
            ConsoleLogger.Default.LogInformation($"JavaPackageDirectory: {this.JavaPackageDirectory}"); // /home/azureuser/VirtualClient.13.6.1/content/linux-x64/packages/microsoft-jdk-17.0.5/linux-x64

            switch (this.Platform)
            {
                case PlatformID.Unix:
                    this.ExecutablePath = this.PlatformSpecifics.Combine(this.PackageDirectory, "bin/hadoop");
                    break;

                default:
                    throw new WorkloadException(
                        $"The Hadoop workload is not supported on the current platform/architecture " +
                        $"{PlatformSpecifics.GetPlatformArchitectureName(this.Platform, this.CpuArchitecture)}." +
                        ErrorReason.PlatformNotSupported);
            }

            this.SetEnvironmentVariable(EnvironmentVariable.JAVA_HOME, this.JavaPackageDirectory, EnvironmentVariableTarget.Process);
            this.SetEnvironmentVariable(EnvironmentVariable.HADOOP_MAPRED_HOME, this.PackageDirectory, EnvironmentVariableTarget.Process);

            ConsoleLogger.Default.LogInformation($"GetEnvironmentVariable: {this.GetEnvironmentVariable("JAVA_HOME")}");  // /home/azureuser/VirtualClient-25/content/linux-x64/packages/microsoft-jdk-11.0.19/linux-x64
            ConsoleLogger.Default.LogInformation($"GetEnvironmentVariable: {this.GetEnvironmentVariable("HADOOP_MAPRED_HOME")}");  // /home/azureuser/VirtualClient-25/content/linux-x64/packages/microsoft-jdk-11.0.19/linux-x64

            string makeFilePath = this.PlatformSpecifics.Combine(this.PackageDirectory, "etc", "hadoop");
            string hadoopEnvFilePath = this.PlatformSpecifics.Combine(makeFilePath, "hadoop-env.sh");

            this.fileSystem.File.ReplaceInFileAsync(
                        hadoopEnvFilePath, @"# export JAVA_HOME=", $"export JAVA_HOME={this.JavaPackageDirectory}", cancellationToken);

            // this.ConfigurationFilesAsync(telemetryContext, cancellationToken);

            await this.systemManagement.MakeFileExecutableAsync(this.ExecutablePath, this.Platform, cancellationToken)
                .ConfigureAwait(false);

            using (IProcessProxy process = await this.ExecuteCommandAsync("apt-get install ssh", null, this.PackageDirectory, telemetryContext, cancellationToken, true)
                   .ConfigureAwait(false))
            {
                if (!cancellationToken.IsCancellationRequested)
                {
                    // await this.LogProcessDetailsAsync(process, telemetryContext, "7Zip", logToFile: true);
                    ConsoleLogger.Default.LogInformation($"command: {process}");
                    process.ThrowIfWorkloadFailed();
                    // this.CaptureMetrics(process, telemetryContext, commandLineArguments);
                }
            }

            string timestamp = DateTime.Now.ToString("ddMMyyHHmmss");

            List<string> commands = new List<string>
            {
                $"mkdir input-{timestamp}",
                $"cp etc/hadoop/capacity-scheduler.xml input-{timestamp}",
                $"cp etc/hadoop/core-site.xml input-{timestamp}",
                $"cp etc/hadoop/hadoop-policy.xml input-{timestamp}",
                $"cp etc/hadoop/hdfs-rbf-site.xml input-{timestamp}",
                $"cp etc/hadoop/hdfs-site.xml input-{timestamp}",
                $"cp etc/hadoop/httpfs-site.xml input-{timestamp}",
                $"cp etc/hadoop/kms-acls.xml input-{timestamp}",
                $"cp etc/hadoop/kms-site.xml input-{timestamp}",
                $"cp etc/hadoop/mapred-site.xml input-{timestamp}",
                $"cp etc/hadoop/yarn-site.xml input-{timestamp}",
                $"bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.5.jar grep input-{timestamp} output-{timestamp} 'dfs[a-z.]+'",
                $"cat output-{timestamp}/*"
            };

            foreach (string command in commands)
            {
                using (IProcessProxy process = await this.ExecuteCommandAsync(command, null, this.PackageDirectory, telemetryContext, cancellationToken, true)
                   .ConfigureAwait(false))
                {
                    if (!cancellationToken.IsCancellationRequested)
                    {
                        // await this.LogProcessDetailsAsync(process, telemetryContext, "7Zip", logToFile: true);
                        ConsoleLogger.Default.LogInformation($"command: {command}");
                        process.ThrowIfWorkloadFailed();
                        // this.CaptureMetrics(process, telemetryContext, commandLineArguments);
                    }
                }
            }

            // this.CreateNodeFolders(telemetryContext, cancellationToken);

            // TODO: update the bin folder

            // await this.ExecuteCommandAsync("hadoop", "version", this.PlatformSpecifics.Combine(this.PackageDirectory, $"bin"), telemetryContext, cancellationToken);
            // await this.ExecuteCommandAsync("hdfs", "--namenode ", this.PlatformSpecifics.Combine(this.PackageDirectory, $"bin"), telemetryContext, cancellationToken);

            // return Task.FromResult(false);
        }

        /// <summary>
        /// Executes the Hadoop Terasort workload.
        /// </summary>
        protected override async Task<bool> ExecuteAsync(EventContext telemetryContext, CancellationToken cancellationToken)
        {
            // SSH into the machine
            List<string> commands = new List<string>
            {
                "ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa",
                "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys",
                "chmod 0600 ~/.ssh/authorized_keys",
                "chmod u+x bin/hdfs",
                "bin/hdfs namenode -format",
                "bin/hdfs dfs -mkdir /user",
                "bin/hdfs dfs -mkdir /user/azureuser",  // fetch <username>
                "chmod u+x sbin/start-yarn.sh",
                "chmod u+x sbin/stop-yarn.sh"
            };
            // commands.Add("sbin/start-yarn.sh");

            foreach (string command in commands)
            {
                ConsoleLogger.Default.LogInformation($"Executing Command: {command}");

                await this.ExecuteCommandAsync(command, null, this.PackageDirectory, telemetryContext, cancellationToken, true)
                    .ConfigureAwait(false);
            }

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

        private void CreateNodeFolders(EventContext telemetryContext, CancellationToken cancellationToken)
        {
            ConsoleLogger.Default.LogInformation($"CreateNodeFolders");

            // string directoryPath = this.PlatformSpecifics.Combine()
            string dataDirectory = this.PlatformSpecifics.Combine(this.PackageDirectory, "data");
            this.fileSystem.Directory.CreateDirectory(dataDirectory);

            string dataNodeDirectory = this.PlatformSpecifics.Combine(this.PackageDirectory, "data", "datanode");
            string nameNodeDirectory = this.PlatformSpecifics.Combine(this.PackageDirectory, "data", "namenode");

            this.fileSystem.Directory.CreateDirectory(dataNodeDirectory);
            this.fileSystem.Directory.CreateDirectory(nameNodeDirectory);
        }
    }
}
