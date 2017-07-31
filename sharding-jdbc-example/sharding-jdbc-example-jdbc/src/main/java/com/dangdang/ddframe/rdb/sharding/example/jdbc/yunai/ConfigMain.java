package com.dangdang.ddframe.rdb.sharding.example.jdbc.yunai;

import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.dangdang.ddframe.rdb.sharding.api.rule.DataSourceRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.api.rule.TableRule;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.DatabaseShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.api.strategy.database.NoneDatabaseShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.NoneTableShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.SingleKeyTableShardingAlgorithm;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.TableShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.jdbc.core.datasource.ShardingDataSource;
import com.google.common.collect.Range;
import org.apache.commons.dbcp.BasicDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * 测试下路由配置
 */
@SuppressWarnings("Duplicates")
public class ConfigMain {

    private static DataSource createDataSource(final String dataSourceName) {
        BasicDataSource result = new BasicDataSource();
        result.setDriverClassName(com.mysql.jdbc.Driver.class.getName());
        result.setUrl(String.format("jdbc:mysql://localhost:33061/%s", dataSourceName)); // TODO 修改：芋艿，数据源
        result.setUsername("root");
        result.setPassword("123456"); // TODO 修改：芋艿，增加密码
        return result;
    }

    private static Map<String, DataSource> buildDataSourceRule(String... dataSourceNames) {
        Map<String, DataSource> results = new HashMap<>();
        for (String dataSourceName : dataSourceNames) {
            results.put(dataSourceName, createDataSource(dataSourceName));
        }
        return results;
    }

    private static Map<String, DataSource> buildDataSourceRule2(String... dataSourceNames) {
        Map<String, DataSource> results = new HashMap<>();
        for (String dataSourceName : dataSourceNames) {
            results.put(dataSourceName, null);
        }
        return results;
    }

    /**
     * 测试单表单库
     */
    private static void testSingleDbSingleTable() {
        // 初始化
        TableRule rule = TableRule
                .builder("t_order")
                .actualTables(Arrays.asList("t_order"))

//                .dataSourceNames(Arrays.asList("single_db_single_table"))
                .dataSourceRule(new DataSourceRule(buildDataSourceRule2("single_db_single_table")))

                .databaseShardingStrategy(new DatabaseShardingStrategy(new NoneKeyModuloDatabaseShardingAlgorithm()))
                .tableShardingStrategy(new TableShardingStrategy("", new NoneTableShardingAlgorithm()))
                .build();
        ShardingRule shardingRule = ShardingRule.builder()
                .tableRules(Arrays.asList(rule))
                .dataSourceRule(new DataSourceRule(buildDataSourceRule("single_db_single_table")))
                .build();
        ShardingDataSource dataSource = new ShardingDataSource(shardingRule);
        // 执行
        String sql = "SELECT * FROM t_order";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    System.out.println(rs.getLong(1) + "\t" + rs.getLong(2) + "\t" + rs.getString(3));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("");
    }

    /**
     * 测试多库多表
     */
    public static void testMultiDbMultiTable() {
        // 初始化
        TableRule rule = TableRule
                .builder("t_order")
                .actualTables(Arrays.asList("t_order_01", "t_order_02"))

//                .dataSourceNames(Arrays.asList("single_db_single_table"))
                .dataSourceRule(new DataSourceRule(buildDataSourceRule2("multi_db_multi_table_01", "multi_db_multi_table_02")))

                .databaseShardingStrategy(new DatabaseShardingStrategy(new NoneKeyModuloDatabaseShardingAlgorithm()))
                .tableShardingStrategy(new TableShardingStrategy("", new NoneTableShardingAlgorithm()))
                .build();

        TableRule rule2 = TableRule
                .builder("t_order_item")
                .actualTables(Arrays.asList("t_order_item_01", "t_order_item_02"))

//                .dataSourceNames(Arrays.asList("single_db_single_table"))
                .dataSourceRule(new DataSourceRule(buildDataSourceRule2("multi_db_multi_table_01", "multi_db_multi_table_02")))

                .databaseShardingStrategy(new DatabaseShardingStrategy(new NoneKeyModuloDatabaseShardingAlgorithm()))
                .tableShardingStrategy(new TableShardingStrategy("", new NoneTableShardingAlgorithm()))
                .build();

        ShardingRule shardingRule = ShardingRule.builder()
                .tableRules(Arrays.asList(rule, rule2))
                .dataSourceRule(new DataSourceRule(buildDataSourceRule("multi_db_multi_table_01", "multi_db_multi_table_02")))
                .build();
        ShardingDataSource dataSource = new ShardingDataSource(shardingRule);
        // 执行
        String sql = "SELECT * FROM t_order";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    System.out.println(rs.getLong(1) + "\t" + rs.getLong(2) + "\t" + rs.getString(3));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("");
    }

    /**
     * 测试单表单库
     */
    private static void testDynamicTable() {
        // 初始化
        TableRule rule = TableRule
                .builder("t_order")
                .actualTables(Arrays.asList("t_order"))

                .dynamic(true)

//                .dataSourceNames(Arrays.asList("single_db_single_table"))
                .dataSourceRule(new DataSourceRule(buildDataSourceRule("single_db_single_table", "single_db_single_table1", "single_db_single_table2")))
//                .dataSourceRule(new DataSourceRule(buildDataSourceRule("single_db_single_table")))

                .databaseShardingStrategy(new DatabaseShardingStrategy("user_id", new NoneDatabaseShardingAlgorithm()))
                .tableShardingStrategy(new TableShardingStrategy("order_id", new SingleKeyTableShardingAlgorithm<Long>() {

                    private final String tablePrefix = "t_order";

                    @Override
                    public String doEqualSharding(final Collection<String> availableTargetNames, final ShardingValue<Long> shardingValue) {
                        return tablePrefix + shardingValue.getValue() % 10;
                    }

                    @Override
                    public Collection<String> doInSharding(final Collection<String> availableTargetNames, final ShardingValue<Long> shardingValue) {
                        Collection<String> result = new LinkedHashSet<>(shardingValue.getValues().size());
                        for (Long value : shardingValue.getValues()) {
                            result.add(tablePrefix + value % 10);
                        }
                        return result;
                    }

                    @Override
                    public Collection<String> doBetweenSharding(final Collection<String> availableTargetNames, final ShardingValue<Long> shardingValue) {
                        Collection<String> result = new LinkedHashSet<>(availableTargetNames.size());
                        Range<Long> range = shardingValue.getValueRange();
                        for (Long i = range.lowerEndpoint(); i <= range.upperEndpoint(); i++) {
                            result.add(tablePrefix + i % 10);
                        }
                        return result;
                    }
                }))
                .build();
        ShardingRule shardingRule = ShardingRule.builder()
                .tableRules(Arrays.asList(rule))
                .dataSourceRule(new DataSourceRule(buildDataSourceRule("single_db_single_table")))
                .build();
        ShardingDataSource dataSource = new ShardingDataSource(shardingRule);
        // 执行
        String sql = "SELECT * FROM t_order WHERE order_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, 1L);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    System.out.println(rs.getLong(1) + "\t" + rs.getLong(2) + "\t" + rs.getString(3));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("");
    }

    public static void main(String[] args) {
//        testSingleDbSingleTable();
//        testDynamicTable();
        testMultiDbMultiTable();
    }

}
