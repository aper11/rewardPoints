package rewardpoints;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author apervala
 * @since Jul 29, 2019
 *
 */
public class ETLInstance {
    private static final Logger log= Logger.getLogger(ETLInstance.class);

    static BigDecimal FIFTY = new BigDecimal("50.00");

    static BigDecimal HUNDRED = new BigDecimal("100.00");

    static BigDecimal ZERO = new BigDecimal("0.00");

    static BigDecimal TWO = new BigDecimal("2");

    private final String dataFile;

    private Map<String, Map<String, BigDecimal>> summary = new HashMap<String, Map<String, BigDecimal>>();;

    private Set<String> allMonths = new TreeSet<>();;

    public ETLInstance(String file) {
        this.dataFile = file;

    }

    public void processRecords() throws IOException {

        InputStream is = loadInputStream(dataFile);

        try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
            String line = br.readLine(); // ignore first line
            while ((line = br.readLine()) != null) {
                try {
                    if (line.startsWith("#"))
                        continue; // ignore comments
                    handleRecord(line);
                }
                catch (Exception e) {
                    log.error("Unable to parse line ->  "+line);
                }
            }
        }

    }

    public Map<String, Map<String, BigDecimal>> getSummary() {
        return summary;
    }

    public Set<String> getAllMonths() {
        return allMonths;
    }

    /**
     * @param file
     * @return
     */
    private static InputStream loadInputStream(String file) {
        ClassLoader cl = Main.class.getClassLoader();
        InputStream is = cl.getResourceAsStream(file);
        return is;
    }

    void handleRecord(String txnLine) throws Exception {
        String[] lineArray = txnLine.split(",");
        String customer = lineArray[0];
        String month = lineArray[1].substring(0, lineArray[1].lastIndexOf("-")); // extract year and month only
        allMonths.add(month);

        BigDecimal amount = new BigDecimal(lineArray[2]);
        BigDecimal reward;

        // A customer receives 2 points for every dollar spent over $100 in each transaction,
        // plus 1 point for every dollar spent over $50 in each transaction
        // (e.g. a $120 purchase = 2x$20 + 1x$50 = 90 points).
        if (amount.compareTo(HUNDRED) > 0) {
            reward = FIFTY.add((amount.subtract(HUNDRED)).multiply(TWO));
        }
        else if (amount.compareTo(FIFTY) > 0) {
            reward = amount.subtract(FIFTY);
        }
        else {
            reward = ZERO;
        }

        Map<String, BigDecimal> customerRewards = summary.computeIfAbsent(customer, k -> new HashMap<String, BigDecimal>());
        BigDecimal customerRewardMonth = customerRewards.computeIfAbsent(month, k -> ZERO);
        customerRewards.put(month, customerRewardMonth.add(reward));

    }

}
