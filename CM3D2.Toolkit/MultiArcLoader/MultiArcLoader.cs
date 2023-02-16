using CM3D2.Toolkit.Guest4168Branch.Arc;
using CM3D2.Toolkit.Guest4168Branch.Arc.Entry;
using CM3D2.Toolkit.Guest4168Branch.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace CM3D2.Toolkit.Guest4168Branch.MultiArcLoader
{
    public class MultiArcLoader : IDisposable
    {
        //Instance Properties
        string[] directoriesCM { get; set; }
        string[] directoriesCOM20 { get; set; }
        string[] directoriesCOM { get; set; }
        string[] directoriesCOMMods { get; set; }
        int threadCount { get; set; }
        Exclude exclusions { get; set; }
        bool hierarchyOnly { get; set; }
        string hierarchyPath { get; set; }
        bool keepDupes { get; set; }
        LoadMethod loadMethod { get; set; } 
        ILogger log { get; set; }

        //Thread Properties
        List<List<string>> arcFilePathsDivided { get; set; }
        List<ArcFileSystem> arcFilePathsDividedArcs { get; set; }
        public ArcFileSystem arc { get; set; }
        List<string> arcFilePaths { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="cmGameData"></param>
        /// <param name="comGameData20"></param>
        /// <param name="comGameData"></param>
        /// <param name="comMods"></param>
        /// <param name="threads"></param>
        /// <param name="loadingMethod"></param>
        /// <param name="hierarchyOnlyFromCache"></param>
        /// <param name="hierachyCachePath"></param>
        /// <param name="keepDuplicates"></param>
        /// <param name="exclude"></param>
        /// <param name="logger"></param>
        public MultiArcLoader(string[] cmGameData, string[] comGameData20, string[] comGameData, string[] comMods, int threads, LoadMethod loadingMethod = LoadMethod.Single, bool hierarchyOnlyFromCache = false, string hierachyCachePath = null, bool keepDuplicates = false, Exclude exclude = Exclude.None, ILogger logger = null)
        {
            directoriesCM = cmGameData;
            directoriesCOM20 = comGameData20;
            directoriesCOM = comGameData;
            directoriesCOMMods = comMods;
            threadCount = Math.Max(1, threads);
            loadMethod = loadingMethod;
            hierarchyOnly = hierarchyOnlyFromCache;
            hierarchyPath = hierachyCachePath;
            keepDupes = keepDuplicates;
            exclusions = exclude;
            log = logger;

            if (log == null)
            {
                log = NullLogger.Instance;
            }
        }

        #region Additional Constructors
        public MultiArcLoader(string[] cmGameData, string[] comGameData20, string[] comGameData, string[] comMods, int threads, LoadMethod loadingMethod = LoadMethod.Single, bool keepDuplicates = false, Exclude exclude = Exclude.None, ILogger logger = null) : this(cmGameData, comGameData20, comGameData, comMods, threads, loadingMethod, false, null, keepDuplicates, exclude, logger)
        {
        }

        public MultiArcLoader(string[] cmGameData, string[] comGameData20, string[] comGameData, string[] comMods, int threads, LoadMethod loadingMethod = LoadMethod.Single, bool hierarchyOnlyFromCache = false, bool keepDuplicates = false, Exclude exclude = Exclude.None) : this(cmGameData, comGameData20, comGameData, comMods, threads, loadingMethod, hierarchyOnlyFromCache, null, keepDuplicates, exclude, null)
        {
        }
        public MultiArcLoader(string[] cmGameData, string[] comGameData20, string[] comGameData, string[] comMods, int threads, LoadMethod loadingMethod = LoadMethod.Single, bool hierarchyOnlyFromCache = false, bool keepDuplicates = false, Exclude exclude = Exclude.None, ILogger logger = null) : this(cmGameData, comGameData20, comGameData, comMods, threads, loadingMethod, hierarchyOnlyFromCache, null, keepDuplicates, exclude, logger)
        {
        }
        #endregion


        public void LoadArcs()
        {
            log.GuestLevel1("MultiArcLoader: LoadArcs Begin");

            //Combine all arc file paths and sort
            arcFilePaths = new List<string>();
            if (directoriesCM != null)
            {
                arcFilePaths.AddRange(fetchSortedArcs(directoriesCM, exclusions, log));
            }
            if (directoriesCOM20 != null)
            {
                arcFilePaths.AddRange(fetchSortedArcs(directoriesCOM20, exclusions, log));
            }
            if (directoriesCOM != null)
            {
                arcFilePaths.AddRange(fetchSortedArcs(directoriesCOM, exclusions, log));
            }
            if (directoriesCOMMods != null)
            {
                arcFilePaths.AddRange(fetchSortedArcs(directoriesCOMMods, exclusions, log));
            }

            //Build from cache if possible
            if (hierarchyOnly && hierarchyPath != null)
            {
                //File Exists
                if (System.IO.File.Exists(hierarchyPath))
                {
                    log.GuestLevel1("MultiArcLoader: LoadArcs LoadCachedHeirarchy Begin");

                    //Load from file
                    MultiArcLoaderHierarchyCache cache = Newtonsoft.Json.JsonConvert.DeserializeObject<MultiArcLoaderHierarchyCache>(File.ReadAllText(hierarchyPath));
                    List<string> cacheFiles = cache.data.Keys.ToList<string>();
                    cacheFiles.Sort();

                    //List of arcs match
                    if (cacheFiles.SequenceEqual(arcFilePaths))
                    {
                        log.GuestLevel1("MultiArcLoader: CachedHeirarchy File List Matches Target");

                        bool buildFromCache = true;

                        //Loop the arc files from cache
                        foreach (KeyValuePair<string, MultiArcLoaderHierarchyCache.Data> cacheKvp in cache.data)
                        {
                            string arcPath = cacheKvp.Key;

                            //File still exists
                            log.GuestLevel4("MultiArcLoader: CachedHeirarchy Checking Arc File: {0}", arcPath);
                            if (System.IO.File.Exists(arcPath))
                            {
                                //Check date modified
                                FileInfo fi = new FileInfo(arcPath);
                                if (!fi.LastWriteTimeUtc.Equals(cacheKvp.Value.dte))
                                {
                                    log.Info("MultiArcLoader: ARC File has been modified (UTC) since last cache:{0} Cache: {1} File: {2}", arcPath, cacheKvp.Value.dte.ToString(), fi.LastWriteTimeUtc);
                                    log.GuestLevel1("MultiArcLoader: ARC File has been modified (UTC) since last cache:{0} Cache: {1} File: {2}", arcPath, cacheKvp.Value.dte.ToString(), fi.LastWriteTimeUtc);
                                    buildFromCache = false;
                                    break;
                                }
                            }
                            else
                            {
                                log.Error("MultiArcLoader: ARC File not found:{0}", arcPath);
                                log.GuestLevel1("MultiArcLoader: ARC File not found:{0}", arcPath);
                                buildFromCache = false;
                                break;
                            }
                        }

                        //Build the actual ARC
                        if (buildFromCache)
                        {
                            log.GuestLevel1("MultiArcLoader: LoadArcs LoadCachedHeirarchy Build Begin");
                            arc = buildHierarchyOnlyArc(cache);
                            log.GuestLevel1("MultiArcLoader: LoadArcs LoadCachedHeirarchy Build End");
                            log.GuestLevel1("MultiArcLoader: LoadArcs LoadCachedHeirarchy End");
                            log.GuestLevel1("MultiArcLoader: LoadArcs End");
                            return;
                        }
                        else
                        {
                            log.GuestLevel1("MultiArcLoader: LoadArcs LoadCachedHeirarchy Failed");
                        }
                    }
                    else
                    {
                        log.Error("MultiArcLoader: ARC File list does not match");

                        log.GuestLevel1("MultiArcLoader: CachedHeirarchy File List DOES NOT Match Target");
                        log.GuestLevel1("MultiArcLoader: LoadArcs LoadCachedHeirarchy Failed");
                    }

                    log.GuestLevel1("MultiArcLoader: LoadArcs LoadCachedHeirarchy End");
                }
                else
                {
                    log.Info("MultiArcLoader: ARC Hierarchy File not found:{0}", hierarchyPath);
                    log.GuestLevel1("MultiArcLoader: ARC Hierarchy File not found:{0}", hierarchyPath);
                }

                log.Info("MultiArcLoader: LoadArcs will now build the full data");
                log.GuestLevel1("MultiArcLoader: LoadArcs will now build the full data");
            }

            //Divide work up based on size
            log.GuestLevel1("MultiArcLoader: LoadArcs Divide Work Begin");
            //Could have used some object, but i'm lazy so multiple lists
            arcFilePathsDivided = new List<List<string>>();
            arcFilePathsDividedArcs = new List<ArcFileSystem>();
            List<long> arcFilePathsDividedSizes = new List<long>();
            for (int i = 0; i < threadCount; i++)
            {
                arcFilePathsDivided.Add(new List<string>());
                arcFilePathsDividedArcs.Add(null);
                arcFilePathsDividedSizes.Add(0);
            }

            //Loop all arcs
            foreach (string filePath in arcFilePaths)
            {
                log.GuestLevel4("MultiArcLoader: Loading FileInfo {0}", filePath);
                FileInfo nextFile = new FileInfo(filePath);

                //Smallest collection gets next arc
                int threadIndex = arcFilePathsDividedSizes.IndexOf(arcFilePathsDividedSizes.Min());
                arcFilePathsDivided[threadIndex].Add(filePath);
                arcFilePathsDividedSizes[threadIndex] = arcFilePathsDividedSizes[threadIndex] + nextFile.Length;
                log.GuestLevel4("MultiArcLoader: Assigning to Thread {0}", threadIndex);
            }
            log.GuestLevel1("MultiArcLoader: LoadArcs Divide Work End");

            //Tasks
            log.GuestLevel1("MultiArcLoader: LoadArcs Threads Begin");
            Task[] tasks = new Task[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                tasks[i] = Task.Factory.StartNew(loadArcsInTask, i.ToString());
            }

            log.GuestLevel1("MultiArcLoader: LoadArcs Threads Waiting");
            Task.WaitAll(tasks);
            log.GuestLevel1("MultiArcLoader: LoadArcs Threads End");

            //After finishing threads, copy everything to single ARC
            log.GuestLevel1("MultiArcLoader: LoadArcs Merge Begin");
            arc = null; 
            for (int i = 0; i < threadCount; i++)
            {
                if (arc == null)
                {
                    arc = arcFilePathsDividedArcs[i];
                }
                else if(arcFilePathsDividedArcs[i] != null)
                {
                    arc.MergeCopy(arcFilePathsDividedArcs[i].Root, arc.Root);
                }
            }
            log.GuestLevel1("MultiArcLoader: LoadArcs Merge End");

            //Build a cache if necessary
            if (hierarchyPath != null)
            {
                log.GuestLevel1("MultiArcLoader: LoadArcs Write New Cache Begin");

                MultiArcLoaderHierarchyCache hierarchy = new MultiArcLoaderHierarchyCache();

                //Loop file paths
                log.GuestLevel1("MultiArcLoader: Checking Arc Paths");
                foreach (string filePath in arcFilePaths)
                {
                    log.GuestLevel4("MultiArcLoader: Checking Arc {0}", filePath);

                    //Add arc file name if necessary
                    if (!hierarchy.data.ContainsKey(filePath))
                    {
                        hierarchy.data[filePath] = new MultiArcLoaderHierarchyCache.Data();
                        hierarchy.data[filePath].dte = new FileInfo(filePath).LastWriteTimeUtc;

                        log.GuestLevel4("MultiArcLoader: Adding Arc UTC {0}", hierarchy.data[filePath].dte.ToString("yyyy-MM-dd HH:mm:ss"));
                    }
                }

                //Loop files
                log.GuestLevel1("MultiArcLoader: Checking Files");
                foreach (KeyValuePair<string, ArcFileEntry> kvp in arc.Files)
                {
                    string filePath = ((Arc.FilePointer.ArcFilePointer)kvp.Value.Pointer).ArcFile;

                    log.GuestLevel4("MultiArcLoader: Checking Arc {0}", filePath);

                    //Add arc file name if necessary
                    if (!hierarchy.data.ContainsKey(filePath))
                    {
                        hierarchy.data[filePath] = new MultiArcLoaderHierarchyCache.Data();
                        hierarchy.data[filePath].dte = new FileInfo(filePath).LastWriteTimeUtc;

                        log.GuestLevel4("MultiArcLoader: Adding Arc UTC {0}", hierarchy.data[filePath].dte.ToString("yyyy-MM-dd HH:mm:ss"));
                    }

                    //Add files contained in arc
                    hierarchy.data[filePath].files.Add(kvp.Value.FullName);
                    log.GuestLevel4("MultiArcLoader: Adding File {0}", kvp.Value.FullName);
                }

                //Write file
                log.GuestLevel1("MultiArcLoader: LoadArcs Write New Cache JSON File Begin");
                File.WriteAllText(hierarchyPath, Newtonsoft.Json.JsonConvert.SerializeObject(hierarchy));
                log.GuestLevel1("MultiArcLoader: LoadArcs Write New Cache JSON File End");

                log.GuestLevel1("MultiArcLoader: LoadArcs Write New Cache End");
            }

            log.GuestLevel1("MultiArcLoader: LoadArcs End");
        }

        private static List<string> fetchSortedArcs(string[] dirs, Exclude exclusions, ILogger log)
        {
            List<string> list = new List<string>();

            foreach (string dir in dirs)
            {
                //Passed arc directly
                if (dir.EndsWith(".arc"))
                {
                    string fileName = Path.GetFileName(dir);
                    if (includeArc(fileName, exclusions, log))
                    {
                        list.Add(dir);
                    }
                }
                //Passed directory
                else
                {
                    string[] files = Directory.GetFiles(dir, "*.arc", SearchOption.AllDirectories);
                    foreach (string file in files)
                    {
                        string fileName = Path.GetFileName(file);
                        if (includeArc(fileName, exclusions, log))
                        {
                            list.Add(file);
                        }
                    }
                }
            }
            list.Sort();

            return list;
        }
        private static bool includeArc(string arcFileName, Exclude exclusions, ILogger log)
        {
            arcFileName = arcFileName.ToLower().Trim();

            if ((exclusions & Exclude.None) == Exclude.None)
            {
                return true;
            }

            if ((exclusions & Exclude.BG) == Exclude.BG && arcFileName.StartsWith("bg"))
            {
                log.GuestLevel4("MultiArcLoader: Exclude ARC Type {0} File: {1}", "bg", arcFileName);
                return false;
            }
            if ((exclusions & Exclude.CSV) == Exclude.CSV && arcFileName.StartsWith("csv"))
            {
                log.GuestLevel4("MultiArcLoader: Exclude ARC Type {0} File: {1}", "csv", arcFileName);
                return false;
            }
            if ((exclusions & Exclude.Motion) == Exclude.Motion && arcFileName.StartsWith("motion"))
            {
                log.GuestLevel4("MultiArcLoader: Exclude ARC Type {0} File: {1}", "motion", arcFileName);
                return false;
            }
            if ((exclusions & Exclude.Parts) == Exclude.Parts && arcFileName.StartsWith("parts"))
            {
                log.GuestLevel4("MultiArcLoader: Exclude ARC Type {0} File: {1}", "parts", arcFileName);
                return false;
            }
            if ((exclusions & Exclude.PriorityMaterial) == Exclude.PriorityMaterial && arcFileName.StartsWith("prioritymaterial"))
            {
                log.GuestLevel4("MultiArcLoader: Exclude ARC Type {0} File: {1}", "prioritymaterial", arcFileName);
                return false;
            }
            if ((exclusions & Exclude.Script) == Exclude.Script && arcFileName.StartsWith("script"))
            {
                log.GuestLevel4("MultiArcLoader: Exclude ARC Type {0} File: {1}", "script", arcFileName);
                return false;
            }
            if ((exclusions & Exclude.Sound) == Exclude.Sound && arcFileName.StartsWith("sound"))
            {
                log.GuestLevel4("MultiArcLoader: Exclude ARC Type {0} File: {1}", "sound", arcFileName);
                return false;
            }
            if ((exclusions & Exclude.System) == Exclude.System && arcFileName.StartsWith("system"))
            {
                log.GuestLevel4("MultiArcLoader: Exclude ARC Type {0} File: {1}", "system", arcFileName);
                return false;
            }
            if ((exclusions & Exclude.Voice) == Exclude.Voice && arcFileName.StartsWith("voice"))
            {
                log.GuestLevel4("MultiArcLoader: Exclude ARC Type {0} File: {1}", "voice", arcFileName);
                return false;
            }

            return true;
        }

        private void loadArcsInTask(System.Object i)
        {
            int index = Int32.Parse(i as string);
            List<string> arcFilePaths = arcFilePathsDivided[index];

            log.GuestLevel1("MultiArcLoader: LoadArcs Task {0} Begin", index);
            try
            {
                ArcFileSystem afs = new ArcFileSystem("root", keepDupes);
                afs.Logger = log;

                if (arcFilePaths.Count > 0)
                {
                    //Loop paths
                    foreach (String arcFilePath in arcFilePaths)
                    {
                        log.GuestLevel4("MultiArcLoader: LoadArcs Task {0} Load Arc: {1}", index, arcFilePath);

                        //Load into the next arc
                        switch (loadMethod)
                        {
                            case LoadMethod.Single:
                            {
                                string arcName = Path.GetFileNameWithoutExtension(arcFilePath);
                                ArcDirectoryEntry dir = afs.CreateDirectory(arcName, afs.Root);
                                try
                                {
                                    afs.LoadArc(arcFilePath, dir, true);
                                }
                                catch(Exception ex)
                                {
                                    log.Error("Unhandled Exception:{0}", ex.ToString());
                                }
                                break;
                            }
                            case LoadMethod.MiniTemps:
                            {
                                ArcFileSystem afsTemp = new ArcFileSystem(hierarchyPath, keepDupes);
                                afsTemp.Logger = log;
                                try
                                {
                                    afsTemp.LoadArc(arcFilePath, afsTemp.Root, true);

                                    //Combine to shared arc
                                    afs.MergeCopy(afsTemp.Root, afs.Root);
                                }
                                catch (Exception ex)
                                {
                                    log.Error("Unhandled Exception:{0}", ex.ToString());
                                }
                                
                                break;
                            }
                            //case LoadMethod.SingleIgnoreArcNames:
                            //    {
                            //        afs.LoadArc(arcFilePath, afs.Root);
                            //        break;
                            //    }
                        }
                    }
                }

                //Copy out
                arcFilePathsDividedArcs[index] = afs;
            }
            catch(Exception ex)
            {
                log.Error("Unhandled Exception:{0}", ex.ToString());
                log.GuestLevel1("MultiArcLoader: LoadArcs Task {0} Unhandled Exception:{1}", index, ex.ToString());
                arcFilePathsDividedArcs[index] = null;
            }
            log.GuestLevel1("MultiArcLoader: LoadArcs Task {0} End", index);
        }

        //Post Load Methods
        public string[] GetFileListAtExtension(string extension)
        {
            extension = extension.Trim();
            extension = (extension.StartsWith(".") ? extension.Substring(1) : extension).Trim();

            List<string> data = new List<string>();
            if (arc != null)
            {
                foreach (ArcFileEntry arcFile in arc.Files.Values)
                {
                    if (arcFile.Name.EndsWith("." + extension))
                    {
                        data.Add(arcFile.Name);
                    }
                }
            }

            return data.ToArray();
        }

        public string GetContentsArcFilePath(ArcEntryBase content)
        {
            if (content == null)
            {
                return null;
            }

            if (content.IsFile())
            {
                if (content.Parent != null)
                {
                    return this.GetContentsArcFilePath(content.Parent);
                }

                return null;
            }
            else
            {
                if (((ArcDirectoryEntry)content).Depth == 1)
                {
                    return ((ArcDirectoryEntry)content).ArcPath;
                }

                if (((ArcDirectoryEntry)content).Parent != null)
                {
                    return this.GetContentsArcFilePath(content.Parent);
                }

                return null;
            }
        }

        [System.Flags]
        public enum Exclude
        {
            None = (2 ^ 1),
            BG = (2 ^ 2),
            CSV = (2 ^ 3),
            Motion = (2 ^ 4),
            Parts = (2 ^ 5),
            PriorityMaterial = (2 ^ 6),
            Script = (2 ^ 7),
            Sound = (2 ^ 8),
            System = (2 ^ 0),
            Voice = (2 ^ 10)
        }

        public enum LoadMethod
        {
            Single = 0,
            MiniTemps = 1,
            //SingleIgnoreArcNames = 2
        }

        public class MultiArcLoaderHierarchyCache
        {
            public Dictionary<string, Data> data { get; set; }
            public MultiArcLoaderHierarchyCache()
            {
                data = new Dictionary<string, Data>();
            }

            public class Data
            {
                public DateTime dte { get; set; }
                public List<string> files { get; set; }
                public Data()
                {
                    dte = DateTime.MinValue;
                    files = new List<string>();
                }
            }
        }

        private ArcFileSystem buildHierarchyOnlyArc(MultiArcLoaderHierarchyCache cache)
        {
            int pathPrefix = (@"CM3D2ToolKit:"+ Path.DirectorySeparatorChar + Path.DirectorySeparatorChar).Length;
            string root = null;

            log.GuestLevel1("MultiArcLoader: LoadArcs LoadCachedHeirarchy Build Get Root Begin");
            foreach (KeyValuePair<string, MultiArcLoaderHierarchyCache.Data> kvpArc in cache.data)
            {
                log.GuestLevel4("MultiArcLoader: Check: {0}", kvpArc.Key);
                log.GuestLevel4("MultiArcLoader: Files Found: {0}", kvpArc.Value.files.Count);
                if (kvpArc.Value.files.Count > 0)
                {
                    root = kvpArc.Value.files[0].Substring(pathPrefix).Split(Path.DirectorySeparatorChar)[0];
                    log.GuestLevel1("MultiArcLoader: Found Root:{0}", root);
                    break;
                }
            }
            log.GuestLevel1("MultiArcLoader: LoadArcs LoadCachedHeirarchy Build Get Root End");

            log.GuestLevel1("MultiArcLoader: LoadArcs LoadCachedHeirarchy Build Construct ARC Begin");
            ArcFileSystem arcH = new ArcFileSystem(root, false);
            foreach(KeyValuePair<string, MultiArcLoaderHierarchyCache.Data> kvpArc in cache.data)
            {
                foreach(string fullPath in kvpArc.Value.files)
                {
                    string fixedPath = fullPath.Substring(pathPrefix).Substring(root.Length);

                    log.GuestLevel4("MultiArcLoader: Creating file: {0}", fixedPath);
                    ArcFileEntry newFile = arcH.CreateFile(fixedPath);
                    if(newFile == null)
                    {
                        log.GuestLevel1("Failed to Create file: {0}", fixedPath);
                    }
                    else
                    {
                        log.GuestLevel5("File created: {0}", fixedPath);
                    }
                }
            }
            log.GuestLevel1("MultiArcLoader: LoadArcs LoadCachedHeirarchy Build Construct ARC End");

            return arcH;
        }

        public void Dispose()
        {
            Dispose(true);

            GC.SuppressFinalize(this);
        }

        private bool disposed = false;
        protected virtual void Dispose(bool disposing)
        {
            if(!this.disposed)
            {
                if(disposing)
                {
                    if (arc != null)
                    {
                        _dispose(arc.Root);
                    }
                }
                this.disposed = true;
            }
        }
        protected static void _dispose(ArcDirectoryEntry dir)
        {
            //Dispose Files
            foreach (ArcFileEntry file in dir.Files.Values)
            {
                file.Pointer.Dispose();
            }
            dir.Files.Clear();

            //Dispose Files in Directories
            foreach (ArcDirectoryEntry dir2 in dir.Directories.Values)
            {
                _dispose(dir2);
            }
            dir.Directories.Clear();
        }
    }
}