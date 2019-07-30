package rewardpoints;

import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


public class Main {
    private static final Logger log = Logger.getLogger(Main.class);
    /**
     * @param args
     */
    public static void main(String[] args) {

        try {

            ETLInstance etl = new ETLInstance("dataset.csv");
            etl.processRecords();
            printResults(etl.getSummary(), etl.getAllMonths());

        }
        catch (Exception e) {
            log.error("Unable to load file: "+e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * @param resultSummary
     * @param months
     */
    private static void printResults(Map<String, Map<String, BigDecimal>> resultSummary, Set<String> months) {

        for (String customer : new TreeSet<>(resultSummary.keySet())) {
            System.out.println(String.format("\n Person: %s", customer));
            BigDecimal totalRewards = ETLInstance.ZERO;
            for (String month : months) {
                BigDecimal reward = resultSummary.get(customer).computeIfAbsent(month , k -> ETLInstance.ZERO);
                totalRewards = totalRewards.add(reward);
                System.out.println(
                        String.format("\t For month: %s, the points earned: %s pts", month,   reward  ));
            }
            System.out.println(String.format("\tTotal rewards: %s pts", totalRewards));

        }

    }

}
