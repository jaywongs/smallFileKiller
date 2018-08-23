package io.trasnwarp.idc.tar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.*;

public class TarFileSystem extends FileSystem {

    public static final String METADATA_CACHE_ENTRIES_KEY = "tar.metadatacache.entries";
    public static final int METADATA_CACHE_ENTRIES_DEFAULT = 10;

    private static Map<URI, TarMetaData> tarMetaCache;

    private URI uri;
    private Path archivePath;

    private String tarAuth;

    private TarMetaData metadata;

    private FileSystem fs;

    Map<Path, TarStatus> archive = new HashMap<Path, TarStatus>();


    public TarFileSystem() {
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        TarStatus hstatus = getFileTarStatus(f);
        if (hstatus.isDir()) {
            throw new FileNotFoundException(f + " : not a file in " +
                    archivePath);
        }
        return new TarFSDataInputStream(fs, new Path(archivePath,
                hstatus.getPartName()),
                hstatus.getStartIndex(), hstatus.getLength(), bufferSize);
    }

    @Override
    public FileSystem[] getChildFileSystems() {
        return new FileSystem[]{fs};
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                     short replication, long blockSize, Progressable progress) throws IOException {
        return null;
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException ie) {
            }
        }
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return false;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return false;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        List<FileStatus> statuses = new ArrayList<FileStatus>();
        Path tmpPath = makeQualified(f);
        Path tarPath = getPathInTar(tmpPath);
        TarStatus hstatus = archive.get(tarPath);
        if (hstatus == null) {
            throw new FileNotFoundException("File " + f + " not found in " + archivePath);
        }
        if (hstatus.isDir()) {
            fileStatusesInIndex(hstatus, statuses);
        } else {
            statuses.add(toFileStatus(hstatus, null));
        }

        return statuses.toArray(new FileStatus[statuses.size()]);
    }

    @Override
    public Path getHomeDirectory() {
        return new Path(uri.toString());
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {

    }

    @Override
    public Path getWorkingDirectory() {
        return new Path(uri.toString());
    }

    public String getTarAuth(URI underLyingUri) {
        String auth = underLyingUri.getScheme() + "-";
        if (underLyingUri.getHost() != null) {
            if (underLyingUri.getUserInfo() != null) {
                auth += underLyingUri.getUserInfo();
                auth += "@";
            }
            auth += underLyingUri.getHost();
            if (underLyingUri.getPort() != -1) {
                auth += ":";
                auth += underLyingUri.getPort();
            }
        } else {
            auth += ":";
        }
        return auth;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return false;
    }

    @Override
    public Path resolvePath(Path p) throws IOException {
        return fs.resolvePath(p);
    }

    private Path makeRelative(String initial, Path p) {
        String scheme = this.uri.getScheme();
        String authority = this.uri.getAuthority();
        Path root = new Path(Path.SEPARATOR);
        if (root.compareTo(p) == 0)
            return new Path(scheme, authority, initial);
        Path retPath = new Path(p.getName());
        Path parent = p.getParent();
        for (int i = 0; i < p.depth() - 1; i++) {
            retPath = new Path(parent.getName(), retPath);
            parent = parent.getParent();
        }
        return new Path(new Path(scheme, authority, initial),
                retPath.toString());
    }

    private void fileStatusesInIndex(TarStatus parent, List<FileStatus> statuses)
            throws IOException {
        String parentString = parent.getName();
        if (!parentString.endsWith(Path.SEPARATOR)) {
            parentString += Path.SEPARATOR;
        }
        Path tarPath = new Path(parentString);
        int tarlen = tarPath.depth();
        final Map<String, FileStatus> cache = new TreeMap<String, FileStatus>();

        for (TarStatus hstatus : archive.values()) {
            String child = hstatus.getName();
            if ((child.startsWith(parentString))) {
                Path thisPath = new Path(child);
                if (thisPath.depth() == tarlen + 1) {
                    statuses.add(toFileStatus(hstatus, cache));
                }
            }
        }
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst)
            throws IOException {
        FileUtil.copy(this, src, getLocal(getConf()), dst, false, getConf());
    }

    @Override
    public Path makeQualified(Path path) {
        Path fsPath = path;
        if (!path.isAbsolute()) {
            fsPath = new Path(archivePath, path);
        }

        URI tmpURI = fsPath.toUri();
        return new Path(uri.getScheme(), tarAuth, tmpURI.getPath());
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        TarStatus hstatus = getFileTarStatus(f);
        return toFileStatus(hstatus, null);
    }

    private Path getPathInTar(Path path) {
        Path tarPath = new Path(path.toUri().getPath());
        if (archivePath.compareTo(tarPath) == 0)
            return new Path(Path.SEPARATOR);
        Path tmp = new Path(tarPath.getName());
        Path parent = tarPath.getParent();
        while (!(parent.compareTo(archivePath) == 0)) {
            if (parent.toString().equals(Path.SEPARATOR)) {
                tmp = null;
                break;
            }
            tmp = new Path(parent.getName(), tmp);
            parent = parent.getParent();
        }
        if (tmp != null)
            tmp = new Path(Path.SEPARATOR, tmp);
        return tmp;
    }

    private TarStatus getFileTarStatus(Path f) throws IOException {
        Path p = makeQualified(f);
        Path tarPath = getPathInTar(p);
        if (tarPath == null) {
            throw new IOException("Invalid file name: " + f + " in " + uri);
        }
        TarStatus hstatus = archive.get(tarPath);
        if (hstatus == null) {
            throw new FileNotFoundException("File: " + f + " does not exist in " + uri);
        }
        return hstatus;
    }


    private ArrayList<Path> getIndexList(Path archivePath) throws IOException {
        ArrayList<Path> IndexPathList = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(archivePath, false);
        while (it.hasNext()) {
            LocatedFileStatus curFile = it.next();
            if (curFile.getPath().toString().endsWith("_index")) {
                IndexPathList.add(curFile.getPath());
            }
        }
        return IndexPathList;
    }

    @Override
    public String getScheme() {
        return "tar";
    }

    public TarFileSystem(FileSystem fs) {
        this.fs = fs;
        this.statistics = fs.getStatistics(getScheme(), this.getClass());
    }

    private synchronized void initializeMetadataCache(Configuration conf) {
        if (tarMetaCache == null) {
            int cacheSize = conf.getInt(METADATA_CACHE_ENTRIES_KEY, METADATA_CACHE_ENTRIES_DEFAULT);
            tarMetaCache = Collections.synchronizedMap(new LruCache<URI, TarMetaData>(cacheSize));
        }
    }

    public void initialize(URI name, Configuration conf) throws IOException {
        initializeMetadataCache(conf);

        URI underLyingURI = decodeTarURI(name, conf);

        Path tarPath = archivePath(
                new Path(name.getScheme(), name.getAuthority(), name.getPath()));
        if (tarPath == null) {
            throw new IOException("Invalid path for the Tar Filesystem. " + name.toString());
        }
        if (fs == null) {
            fs = FileSystem.get(underLyingURI, conf);
        }
        uri = tarPath.toUri();
        archivePath = new Path(uri.getPath());
        tarAuth = getTarAuth(underLyingURI);

        //indexPathList
        ArrayList<Path> indexPathList = new ArrayList<>();
        indexPathList = getIndexList(archivePath);

        for (Path archiveIndexPath : indexPathList) {
            metadata = tarMetaCache.get(uri);
            if (metadata != null) {
                FileStatus aStat = fs.getFileStatus(archiveIndexPath);
                if (aStat.getModificationTime() != metadata.getArchiveIndexTimestamp()) {
                    metadata = null;
                    tarMetaCache.remove(uri);
                }
            }
            if (metadata == null) {
                metadata = new TarMetaData(fs, archiveIndexPath);
                metadata.parseMetaData();
                tarMetaCache.put(uri, metadata);
            }
        }

    }

    private URI decodeTarURI(URI rawURI, Configuration conf) throws IOException {
        String tmpAuth = rawURI.getAuthority();

        if (tmpAuth == null) {
            //create a path
            return FileSystem.getDefaultUri(conf);
        }
        String authority = rawURI.getAuthority();

        int i = authority.indexOf('-');
        if (i < 0) {
            throw new IOException("URI: " + rawURI
                    + " is an invalid Tar URI since '-' not found."
                    + "  Expecting tar://<scheme>-<host>/<path>.");
        }

        if (rawURI.getQuery() != null) {
            // query component not allowed
            throw new IOException("query component in Path not supported  " + rawURI);
        }

        URI tmp;
        try {
            // convert <scheme>-<host> to <scheme>://<host>
            URI baseUri = new URI(authority.replaceFirst("-", "://"));

            tmp = new URI(baseUri.getScheme(), baseUri.getAuthority(),
                    rawURI.getPath(), rawURI.getQuery(), rawURI.getFragment());
        } catch (URISyntaxException e) {
            throw new IOException("URI: " + rawURI
                    + " is an invalid Tar URI. Expecting Tar://<scheme>-<host>/<path>.");
        }
        return tmp;
    }

    @Override
    public URI getUri() {
        return this.uri;
    }

    @Override
    protected void checkPath(Path path) {
        URI uri = path.toUri();
        String thatScheme = uri.getScheme();
        if (thatScheme == null)                // fs is relative
            return;
        URI thisUri = getCanonicalUri();
        String thisScheme = thisUri.getScheme();
        //authority and scheme are not case sensitive
        if (thisScheme.equalsIgnoreCase(thatScheme)) {// schemes match
            String thisAuthority = thisUri.getAuthority();
            String thatAuthority = uri.getAuthority();
            if (thatAuthority == null &&                // path's authority is null
                    thisAuthority != null) {                // fs has an authority
                URI defaultUri = getDefaultUri(getConf());
                if (thisScheme.equalsIgnoreCase(defaultUri.getScheme())) {
                    uri = defaultUri; // schemes match, so use this uri instead
                } else {
                    uri = null; // can't determine auth of the path
                }
            }
            if (uri != null) {
                // canonicalize uri before comparing with this fs
                uri = canonicalizeUri(uri);
                thatAuthority = uri.getAuthority();
                if (thisAuthority == thatAuthority ||       // authorities match
                        (thisAuthority != null &&
                                thisAuthority.equalsIgnoreCase(thatAuthority)))
                    return;
            }
        }
        throw new IllegalArgumentException("Wrong FS: " + path +
                ", expected: " + this.getUri());
    }

    public Configuration getConf() {
        return fs.getConf();
    }

    private Path archivePath(Path p) {
        Path retPath = null;
        Path tmp = p;
        for (int i = 0; i < p.depth(); i++) {
            if (tmp.toString().endsWith(".tar")) {
                retPath = tmp;
                break;
            }
            tmp = tmp.getParent();
        }
        return retPath;
    }

    private static String decodeString(String str)
            throws UnsupportedEncodingException {
        return URLDecoder.decode(str, "UTF-8");
    }

    private FileStatus toFileStatus(TarStatus t,
                                    Map<String, FileStatus> cache) throws IOException {
        FileStatus underlying = null;
        if (cache != null) {
            underlying = cache.get(t.partName);
        }
        if (underlying == null) {
            final Path p = t.isDir ? archivePath : new Path(archivePath, t.partName);
            underlying = fs.getFileStatus(p);
            if (cache != null) {
                cache.put(t.partName, underlying);
            }
        }

        long modTime = 0;
        modTime = t.getModificationTime();

        return new FileStatus(
                t.isDir() ? 0L : t.getLength(),
                t.isDir(),
                underlying.getReplication(),
                underlying.getBlockSize(),
                modTime,
                underlying.getAccessTime(),
                underlying.getPermission(),
                underlying.getOwner(),
                underlying.getGroup(),
                makeRelative(this.uri.getPath(), new Path(t.name)));
    }

    private class TarStatus {
        boolean isDir;
        String name;
        List<String> children;
        String partName;
        long startIndex;
        long length;
        long modificationTime;

        public TarStatus(String tarString) throws UnsupportedEncodingException {
            String[] splits = tarString.split(" ");
            this.name = decodeString(splits[0]);
            this.isDir = "dir".equals(splits[1]);
            this.partName = splits[2];
            this.startIndex = Long.parseLong(splits[3]);
            this.length = Long.parseLong(splits[4]);

            String[] propSplits = null;
            if (isDir) {
                propSplits = decodeString(this.partName).split(" ");
                children = new ArrayList<String>();
                for (int i = 5; i < splits.length; i++) {
                    children.add(decodeString(splits[i]));
                }
            } else {
                propSplits = decodeString(splits[5]).split(" ");
            }

            if (propSplits != null && propSplits.length >= 4) {
                modificationTime = Long.parseLong(propSplits[0]);
            }
        }

        public boolean isDir() {
            return isDir;
        }

        public String getName() {
            return name;
        }

        public String getPartName() {
            return partName;
        }

        public long getStartIndex() {
            return startIndex;
        }

        public long getLength() {
            return length;
        }

        public long getModificationTime() {
            return modificationTime;
        }

    }


    private static class TarFSDataInputStream extends FSDataInputStream {
        private static class TarFsInputStream extends FSInputStream
                implements CanSetDropBehind, CanSetReadahead {
            private long position, start, end;
            private final FSDataInputStream underLyingStream;
            private final byte[] oneBytebuff = new byte[1];

            TarFsInputStream(FileSystem fs, Path path, long start,
                             long length, int bufferSize) throws IOException {
                if (length < 0) {
                    throw new IllegalArgumentException("Negative length [" + length + "]");
                }
                underLyingStream = fs.open(path, bufferSize);
                underLyingStream.seek(start);
                this.start = start;
                this.position = start;
                this.end = start + length;
            }

            @Override
            public synchronized int available() throws IOException {
                long remaining = end - underLyingStream.getPos();
                if (remaining > Integer.MAX_VALUE) {
                    return Integer.MAX_VALUE;
                }
                return (int) remaining;
            }

            @Override
            public synchronized void close() throws IOException {
                underLyingStream.close();
                super.close();
            }

            @Override
            public void mark(int readLimit) {
            }


            @Override
            public void reset() throws IOException {
                throw new IOException("reset not implemented.");
            }

            @Override
            public synchronized int read() throws IOException {
                int ret = read(oneBytebuff, 0, 1);
                return (ret <= 0) ? -1 : (oneBytebuff[0] & 0xff);
            }

            @Override
            public synchronized int read(byte[] b) throws IOException {
                final int ret = read(b, 0, b.length);
                return ret;
            }

            /**
             *
             */
            @Override
            public synchronized int read(byte[] b, int offset, int len)
                    throws IOException {
                int newlen = len;
                int ret = -1;
                if (position + len > end) {
                    newlen = (int) (end - position);
                }
                // end case
                if (newlen == 0)
                    return ret;
                ret = underLyingStream.read(b, offset, newlen);
                position += ret;
                return ret;
            }

            @Override
            public synchronized long skip(long n) throws IOException {
                long tmpN = n;
                if (tmpN > 0) {
                    final long actualRemaining = end - position;
                    if (tmpN > actualRemaining) {
                        tmpN = actualRemaining;
                    }
                    underLyingStream.seek(tmpN + position);
                    position += tmpN;
                    return tmpN;
                }
                return 0;
            }

            @Override
            public synchronized long getPos() throws IOException {
                return (position - start);
            }

            @Override
            public synchronized void seek(final long pos) throws IOException {
                validatePosition(pos);
                position = start + pos;
                underLyingStream.seek(position);
            }

            private void validatePosition(final long pos) throws IOException {
                if (pos < 0) {
                    throw new IOException("Negative position: " + pos);
                }
                final long length = end - start;
                if (pos > length) {
                    throw new IOException("Position behind the end " +
                            "of the stream (length = " + length + "): " + pos);
                }
            }

            @Override
            public boolean seekToNewSource(long targetPos) throws IOException {

                return false;
            }

            @Override
            public int read(long pos, byte[] b, int offset, int length)
                    throws IOException {
                int nlength = length;
                if (start + nlength + pos > end) {
                    // length corrected to the real remaining length:
                    nlength = (int) (end - start - pos);
                }
                if (nlength <= 0) {
                    // EOS:
                    return -1;
                }
                return underLyingStream.read(pos + start, b, offset, nlength);
            }

            @Override
            public void readFully(long pos, byte[] b, int offset, int length)
                    throws IOException {
                if (start + length + pos > end) {
                    throw new IOException("Not enough bytes to read.");
                }
                underLyingStream.readFully(pos + start, b, offset, length);
            }

            @Override
            public void readFully(long pos, byte[] b) throws IOException {
                readFully(pos, b, 0, b.length);
            }

            @Override
            public void setReadahead(Long readahead) throws IOException {
                underLyingStream.setReadahead(readahead);
            }

            @Override
            public void setDropBehind(Boolean dropBehind) throws IOException {
                underLyingStream.setDropBehind(dropBehind);
            }
        }

        public TarFSDataInputStream(FileSystem fs, Path p, long start,
                                    long length, int bufsize) throws IOException {
            super(new TarFsInputStream(fs, p, start, length, bufsize));
        }
    }

    private class TarMetaData {
        private FileSystem fs;

        // the index file
        private Path archiveIndexPath;
        private long archiveIndexTimestamp;

        //        Map<Path, TarStatus> archive = new HashMap<Path, TarStatus>();
        private Map<Path, FileStatus> partFileStatuses = new HashMap<Path, FileStatus>();

        public TarMetaData(FileSystem fs, Path archiveIndexPath) {
            this.fs = fs;
            this.archiveIndexPath = archiveIndexPath;
        }

        public FileStatus getPartFileStatus(Path partPath) throws IOException {
            FileStatus status;
            status = partFileStatuses.get(partPath);
            if (status == null) {
                status = fs.getFileStatus(partPath);
                partFileStatuses.put(partPath, status);
            }
            return status;
        }

        public long getArchiveIndexTimestamp() {
            return archiveIndexTimestamp;
        }

        private void parseMetaData() throws IOException {
            Text line = new Text();

            FSDataInputStream aIn = fs.open(archiveIndexPath);
            try {
                FileStatus archiveStat = fs.getFileStatus(archiveIndexPath);
                archiveIndexTimestamp = archiveStat.getModificationTime();
                LineReader aLin;

                aIn.seek(0);
                aLin = new LineReader(aIn, getConf());
                while (aLin.readLine(line) != 0) {
                    String lineFeed = line.toString();
                    String[] parsed = lineFeed.split(" ");
                    parsed[0] = decodeString(parsed[0]);
                    archive.put(new Path(parsed[0]), new TarStatus(lineFeed));
                }
            } finally {
                IOUtils.cleanup(LOG, aIn);
            }
        }
    }

    private static class LruCache<K, V> extends LinkedHashMap<K, V> {
        private final int MAX_ENTRIES;

        public LruCache(int maxEntries) {
            super(maxEntries + 1, 1.0f, true);
            MAX_ENTRIES = maxEntries;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > MAX_ENTRIES;
        }
    }
}
