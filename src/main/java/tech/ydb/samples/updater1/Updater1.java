package tech.ydb.samples.updater1;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author zinal
 */
public class Updater1 implements Runnable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Updater1.class);

    private final Connection con;
    private final BufferedReader input;
    private final int batchSize;
    private long lastReportedAt = 0L;
    private long rows = 0L;
    private long transactions = 0L;

    public Updater1(Connection con, BufferedReader input, int batchSize) {
        this.con = con;
        this.input = input;
        this.batchSize = batchSize;
    }

    public long getRows() {
        return rows;
    }

    public long getTransactions() {
        return transactions;
    }

    @Override
    public void run() {
        lastReportedAt = System.currentTimeMillis();
        try {
            List<Long> batch = new ArrayList<>();
            String line;
            while ((line = input.readLine()) != null) {
                line = line.trim();
                if (line.length()==0) {
                    continue;
                }
                batch.add(Long.valueOf(line));
                if (batch.size() >= batchSize) {
                    updateBatch(batch);
                    batch.clear();
                    maybeReport(false);
                }
            }
            updateBatch(batch);
            maybeReport(true);
        } catch(Exception ex) {
            throw new RuntimeException("run() failed", ex);
        }
    }

    private void updateBatch(List<Long> batch) throws Exception {
        String sql = "UPDATE `sm1/EVENTS` SET REMARKS=? WHERE CODE=?";
        try (PreparedStatement ps = con.prepareStatement(sql)) {
            for (Long code : batch) {
                ps.setString(1, "updated #" + code);
                ps.setLong(2, code);
                ps.addBatch();
                rows += 1;
            }
            ps.executeBatch();
        }
        con.commit();
        transactions += 1;
    }

    private boolean maybeReport(boolean term) {
        boolean needReport = term;
        if (!needReport) {
            long tv = System.currentTimeMillis();
            if (tv - lastReportedAt >= 10000L) {
                needReport = true;
                lastReportedAt = tv;
            }
        }
        if (needReport) {
            LOG.info("Progress: {} transactions, {} rows updated.", transactions, rows);
        }
        return needReport;
    }

    private static Properties readProperties(String fileName) {
        byte[] data;
         try {
             data = Files.readAllBytes(Paths.get(fileName));
         } catch (IOException ix) {
             throw new RuntimeException("Failed to read file " + fileName, ix);
         }
         Properties props = new Properties();
         try {
             props.loadFromXML(new ByteArrayInputStream(data));
         } catch (IOException ix) {
             throw new RuntimeException("Failed to parse properties file " + fileName, ix);
         }
         return props;
    }

    private static Connection createConnection(Properties props) throws Exception {
        String url = props.getProperty("ydb.url");
        String username = props.getProperty("ydb.username");
        if (username != null && username.length() > 0) {
            String password = props.getProperty("ydb.password");
            return DriverManager.getConnection(url, username, password);
        } else {
            return DriverManager.getConnection(url);
        }
    }

    public static void main(String[] args) {
        try {
            String configFileName = "updater1.xml";
            String idFileName = "updater1.txt";
            if (args.length > 0) {
                configFileName = args[0];
            }
            if (args.length > 1) {
                idFileName = args[1];
            }
            LOG.info("Reading configuration from {}...", configFileName);
            Properties props = readProperties(configFileName);
            long tvStart, tvFinish, transactions;
            LOG.info("Opening the id file {}...", idFileName);
            try (FileInputStream fis = new FileInputStream(idFileName)) {
                LOG.info("Obtaining the JDBC connection...");
                try ( Connection con = createConnection(props) ) {
                    con.setAutoCommit(false);
                    LOG.info("Creating the worker instance...");
                    Updater1 worker = new Updater1(con,
                            new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8)),
                            1000
                    );
                    LOG.info("Entering the updater algorithm...");
                    tvStart = System.currentTimeMillis();
                    worker.run();
                    transactions = worker.getTransactions();
                    tvFinish = System.currentTimeMillis();
                }
            }
            LOG.info("All done in {} milliseconds, total {} transactions.", 
                    tvFinish - tvStart, transactions);
        } catch(Exception ex) {
            LOG.error("FATAL", ex);
        }
    }

}
