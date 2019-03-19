package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.io.File;
import java.io.FileFilter;
import java.util.Collections;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class FilePageIdsDumpStore implements PageIdsDumpStore {
    /** File extension. */
    private static final String FILE_EXT = ".seg";

    /** File name length. */
    private static final int FILE_NAME_LENGTH = 12;

    /** Group ID prefix length. */
    private static final int GRP_ID_PREFIX_LENGTH = 8;

    /** File name pattern. */
    private static final Pattern FILE_NAME_PATTERN = Pattern.compile("[0-9A-Fa-f]{" + FILE_NAME_LENGTH + "}\\" + FILE_EXT);

    /** File filter. */
    private static final FileFilter FILE_FILTER = file -> !file.isDirectory() && FILE_NAME_PATTERN.matcher(file.getName()).matches();

    /** Page IDs dump directory. */
    private final File dumpDir;

    /** */
    private final IgniteLogger log;

    /**
     * @param dumpDir Page IDs dump directory.
     */
    public FilePageIdsDumpStore(File dumpDir, IgniteLogger log) {
        this.dumpDir = dumpDir;
        this.log = log.getLogger(FilePageIdsDumpStore.class);
    }

    /** {@inheritDoc} */
    @Override public void savePartition(Partition part, int zone) {

    }

    /** {@inheritDoc} */
    @Override public Iterable<Zone> zones() {
        File[] segFiles = dumpDir.listFiles(FILE_FILTER);

        if (segFiles == null) {
            if (log.isInfoEnabled())
                log.info("Saved prewarming dump files not found!");

            return Collections.emptyList();
        }

        if (log.isInfoEnabled())
            log.info("Saved prewarming dump files found: " + segFiles.length);

        for (File segFile : segFiles) { // TODO fix the body!!!
            try {
                int partId = Integer.parseInt(segFile.getName().substring(
                    GRP_ID_PREFIX_LENGTH,
                    FILE_NAME_LENGTH), 16);

                int grpId = (int)Long.parseLong(segFile.getName().substring(0, GRP_ID_PREFIX_LENGTH), 16);

                // TODO
            }
            catch (NumberFormatException e) {
                U.error(log, "Invalid prewarming dump file name: " + segFile.getName(), e);
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void clear() {

    }

    /**
     * @param partKey Partition key.
     */
    private String partFileName(long partKey) {
        SB b = new SB();

        String keyHex = Long.toHexString(partKey);

        for (int i = keyHex.length(); i < FILE_NAME_LENGTH; i++)
            b.a('0');

        return b.a(keyHex).a(FILE_EXT).toString();
    }
}
