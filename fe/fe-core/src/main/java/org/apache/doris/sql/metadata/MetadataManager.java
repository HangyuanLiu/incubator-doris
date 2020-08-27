package org.apache.doris.sql.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.ColumnStats;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.sql.planner.statistics.ColumnStatistics;
import org.apache.doris.sql.planner.statistics.DoubleRange;
import org.apache.doris.sql.planner.statistics.Estimate;
import org.apache.doris.sql.planner.statistics.TableStatistics;
import org.apache.doris.sql.type.TypeManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class MetadataManager implements Metadata {
    private final FunctionManager functions;
    private final TypeManager typeManager;
    //目前只有DorisMetadata，未来可以拓展
    private ConnectorMetadata metadata;

    String[][] statistics = {{"customer", "c_custkey", "0.0", "150000", "10", "1", "150000"},{"customer", "c_name", "0.0", "150000", "25", "Customer#000000001", "Customer#000150000"},{"customer", "c_address", "0.0", "150000", "40", "   2uZwVhQvwA", "zzxGktzXTMKS1BxZlgQ9nqQ"},{"customer", "c_nationkey", "0.0", "25", "10", "0", "24"},{"customer", "c_phone", "0.0", "150000", "15", "10-100-106-1617", "34-999-618-6881"},{"customer", "c_acctbal", "0.0", "140187", "15", "-999.99", "9999.99"},{"customer", "c_mktsegment", "0.0", "5", "10", "AUTOMOBILE", "MACHINERY"},{"customer", "c_comment", "0.0", "149968", "117", " Tiresias according to the slyly blithe instructions detect quickly at the slyly express courts. express dinos wake ", "zzle. blithely regular instructions cajol"},{"customer", "c_end", "0.0", "1", "1", "", ""},{"lineitem", "l_orderkey", "0.0", "1500000", "10", "1", "6000000"},{"lineitem", "l_partkey", "0.0", "200000", "10", "1", "200000"},{"lineitem", "l_suppkey", "0.0", "10000", "10", "1", "10000"},{"lineitem", "l_linenumber", "0.0", "7", "10", "1", "7"},{"lineitem", "l_quantity", "0.0", "50", "15", "1", "50"},{"lineitem", "l_extendedprice", "0.0", "933900", "15", "901", "104949.5"},{"lineitem", "l_discount", "0.0", "11", "15", "0", "0.1"},{"lineitem", "l_tax", "0.0", "9", "15", "0", "0.08"},{"lineitem", "l_returnflag", "0.0", "3", "1", "A", "R"},{"lineitem", "l_linestatus", "0.0", "2", "1", "F", "O"},{"lineitem", "l_shipdate", "0.0", "2526", "None", "1992-01-02", "1998-12-01"},{"lineitem", "l_commitdate", "0.0", "2466", "None", "1992-01-31", "1998-10-31"},{"lineitem", "l_receiptdate", "0.0", "2554", "None", "1992-01-04", "1998-12-31"},{"lineitem", "l_shipinstruct", "0.0", "4", "25", "COLLECT COD", "TAKE BACK RETURN"},{"lineitem", "l_shipmode", "0.0", "7", "10", "AIR", "TRUCK"},{"lineitem", "l_comment", "0.0", "4580667", "44", " Tiresias ", "zzle? slyly final platelets sleep quickly. "},{"lineitem", "l_orderkey", "0.0", "1500000", "10", "1", "6000000"},{"lineitem", "l_partkey", "0.0", "200000", "10", "1", "200000"},{"lineitem", "l_suppkey", "0.0", "10000", "10", "1", "10000"},{"lineitem", "l_linenumber", "0.0", "7", "10", "1", "7"},{"lineitem", "l_quantity", "0.0", "50", "15", "1", "50"},{"lineitem", "l_extendedprice", "0.0", "933900", "15", "901", "104949.5"},{"lineitem", "l_discount", "0.0", "11", "15", "0", "0.1"},{"lineitem", "l_tax", "0.0", "9", "15", "0", "0.08"},{"lineitem", "l_returnflag", "0.0", "3", "1", "A", "R"},{"lineitem", "l_linestatus", "0.0", "2", "1", "F", "O"},{"lineitem", "l_shipdate", "0.0", "2526", "None", "1992-01-02", "1998-12-01"},{"lineitem", "l_commitdate", "0.0", "2466", "None", "1992-01-31", "1998-10-31"},{"lineitem", "l_receiptdate", "0.0", "2554", "None", "1992-01-04", "1998-12-31"},{"lineitem", "l_shipinstruct", "0.0", "4", "25", "COLLECT COD", "TAKE BACK RETURN"},{"lineitem", "l_shipmode", "0.0", "7", "10", "AIR", "TRUCK"},{"lineitem", "l_comment", "0.0", "4580667", "44", " Tiresias ", "zzle? slyly final platelets sleep quickly. "},{"nation", "n_nationkey", "0.0", "25", "10", "0", "24"},{"nation", "n_name", "0.0", "25", "25", "ALGERIA", "VIETNAM"},{"nation", "n_regionkey", "0.0", "5", "10", "0", "4"},{"nation", "n_comment", "0.0", "25", "152", " haggle. carefully final deposits detect slyly agai", "y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be"},{"nation", "o_end", "0.0", "1", "1", "", ""},{"orders", "o_orderkey", "0.0", "1500000", "10", "1", "6000000"},{"orders", "o_custkey", "0.0", "99996", "10", "1", "149999"},{"orders", "o_orderstatus", "0.0", "3", "1", "F", "P"},{"orders", "o_totalprice", "0.0", "1464556", "15", "857.71", "555285.16"},{"orders", "o_orderdate", "0.0", "2406", "None", "1992-01-01", "1998-08-02"},{"orders", "o_orderpriority", "0.0", "5", "15", "1-URGENT", "5-LOW"},{"orders", "o_clerk", "0.0", "1000", "15", "Clerk#000000001", "Clerk#000001000"},{"orders", "o_shippriority", "0.0", "1", "10", "0", "0"},{"orders", "o_comment", "0.0", "1482071", "79", " Tiresias about the blithely ironic a", "zzle? furiously ironic instructions among the unusual t"},{"orders", "o_end", "0.0", "1", "1", "", ""},{"part", "p_partkey", "0.0", "200000", "10", "1", "200000"},{"part", "p_name", "0.0", "199997", "55", "almond antique blue royal burnished", "yellow white seashell lavender black"},{"part", "p_mfgr", "0.0", "5", "25", "Manufacturer#1", "Manufacturer#5"},{"part", "p_brand", "0.0", "25", "10", "Brand#11", "Brand#55"},{"part", "p_type", "0.0", "150", "25", "ECONOMY ANODIZED BRASS", "STANDARD POLISHED TIN"},{"part", "p_size", "0.0", "50", "10", "1", "50"},{"part", "p_container", "0.0", "40", "10", "JUMBO BAG", "WRAP PKG"},{"part", "p_retailprice", "0.0", "20899", "15", "901", "2098.99"},{"part", "p_comment", "0.0", "131753", "23", " Tire", "zzle. quickly si"},{"part", "end", "0.0", "1", "1", "", ""},{"partsupp", "ps_partkey", "0.0", "200000", "10", "1", "200000"},{"partsupp", "ps_suppkey", "0.0", "10000", "10", "1", "10000"},{"partsupp", "ps_availqty", "0.0", "9999", "10", "1", "9999"},{"partsupp", "ps_supplycost", "0.0", "99865", "15", "1", "1000"},{"partsupp", "ps_comment", "0.0", "799124", "199", " Tiresias according to the quiet courts sleep against the ironic, final requests. carefully unusual requests affix fluffily quickly ironic packages. regular ", "zzle. unusual decoys detect slyly blithely express frays. furiously ironic packages about the bold accounts are close requests. slowly silent reque"},{"partsupp", "end", "0.0", "1", "1", "", ""},{"region", "r_regionkey", "0.0", "5", "10", "0", "4"},{"region", "r_name", "0.0", "5", "25", "AFRICA", "MIDDLE EAST"},{"region", "r_comment", "0.0", "5", "152", "ges. thinly even pinto beans ca", "uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl"},{"region", "end", "0.0", "1", "1", "", ""},{"supplier", "s_suppkey", "0.0", "10000", "10", "1", "10000"},{"supplier", "s_name", "0.0", "10000", "25", "Supplier#000000001", "Supplier#000010000"},{"supplier", "s_address", "0.0", "10000", "40", "  9aW1wwnBJJPnCx,nox0MA48Y0zpI1IeVfYZ", "zzfDhdtZcvmVzA8rNFU,Yctj1zBN"},{"supplier", "s_nationkey", "0.0", "25", "10", "0", "24"},{"supplier", "s_phone", "0.0", "10000", "15", "10-102-116-6785", "34-998-900-4911"},{"supplier", "s_acctbal", "0.0", "9955", "15", "-998.22", "9999.72"},{"supplier", "s_comment", "0.0", "10000", "101", " about the blithely express foxes. bli", "zzle furiously. bold accounts haggle furiously ironic excuses. fur"},{"supplier", "end", "0.0", "1", "1", "", ""}};

    public MetadataManager(
            TypeManager typeManager,
            FunctionManager functionManager,
            Catalog catalog) {
        this.typeManager = typeManager;
        this.functions = functionManager;
        this.metadata = new DorisMetadata(catalog, typeManager);
    }

    @Override
    public boolean schemaExists(Session session, CatalogSchemaName schema) {
        return false;
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName table) {
        requireNonNull(table, "table is null");

        //CatalogMetadata catalogMetadata = catalog.get();
        ConnectorId connectorId = new ConnectorId("doris");
        ConnectorTableHandle tableHandle = metadata.getTableHandle(session.toConnectorSession(connectorId), table.asSchemaTableName());
        if (tableHandle != null) {
            return Optional.of(new TableHandle(connectorId, tableHandle));
        }
        return Optional.empty();
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle) {
        ConnectorId connectorId = new ConnectorId("doris");
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());
        return new TableMetadata(tableMetadata);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle) {
        ConnectorId connectorId = tableHandle.getConnectorId();
        Map<String, ColumnHandle> handles = metadata.getColumnHandles(session.toConnectorSession(connectorId), tableHandle.getConnectorHandle());

        ImmutableMap.Builder<String, ColumnHandle> map = ImmutableMap.builder();
        for (Map.Entry<String, ColumnHandle> mapEntry : handles.entrySet()) {
            map.put(mapEntry.getKey().toLowerCase(ENGLISH), mapEntry.getValue());
        }
        return map.build();
    }

    @Override
    public FunctionManager getFunctionManager()
    {
        // TODO: transactional when FunctionManager is made transactional
        return functions;
    }

    @Override
    public TypeManager getTypeManager()
    {
        // TODO: make this transactional when we allow user defined types
        return typeManager;
    }

    @Override
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle, List<ColumnHandle> columnHandles) {
        Map<ColumnHandle, ColumnStatistics> columnStatisticsMap = Maps.newHashMap();
        OlapTable olapTable = (OlapTable)(((DorisTableHandle)tableHandle.getConnectorHandle()).getTable());

        long rowCount = olapTable.getRowCount();
        for (ColumnHandle columnHandle : columnHandles) {
            String columnName = ((DorisColumnHandle)columnHandle).getColumnName();
            for (String[] stas : statistics) {
                if (stas[0].equalsIgnoreCase(olapTable.getName()) && stas[1].equalsIgnoreCase(columnName)) {
                    Estimate nullFraction = Estimate.of(Double.valueOf(stas[2]));
                    Estimate distinctValuesCount = Estimate.of(Double.valueOf(stas[3]));

                    Estimate dataSize;
                    if (stas[4].equalsIgnoreCase("None")) {
                        dataSize = Estimate.of(Double.valueOf("10"));
                    } else {
                        dataSize = Estimate.of(Double.valueOf(stas[4]));
                    }

                    Optional<DoubleRange> range;
                    try {
                        try {
                            DateLiteral dateLiteral1 = new DateLiteral(stas[5], org.apache.doris.catalog.Type.DATE);
                            DateLiteral dateLiteral2 = new DateLiteral(stas[6], org.apache.doris.catalog.Type.DATE);
                            range = Optional.of(new DoubleRange(
                                    dateLiteral1.getLongValue() / 1000000,
                                    dateLiteral2.getLongValue() / 1000000));
                        } catch (Exception ex) {
                            range = Optional.of(new DoubleRange(Double.valueOf(stas[5]), Double.valueOf(stas[6])));
                        }
                    } catch (Exception ex) {
                        range = Optional.of(new DoubleRange(Integer.MIN_VALUE, Integer.MAX_VALUE));
                    }

                    ColumnStatistics statistics = new ColumnStatistics(nullFraction, distinctValuesCount, dataSize, range);
                    columnStatisticsMap.put(columnHandle, statistics);
                }
                /*
                else {
                    DoubleRange range = new DoubleRange(Integer.MIN_VALUE, Integer.MAX_VALUE);
                    ColumnStatistics statistics = new ColumnStatistics(
                            Estimate.zero(),
                            Estimate.of(rowCount),
                            Estimate.of(olapTable.getColumn(((DorisColumnHandle) columnHandle).getColumnName()).getDataType().getSlotSize()),
                            Optional.of(range));
                    columnStatisticsMap.put(columnHandle, statistics);
                }

                 */
            }
        }
        System.out.println(olapTable.getName() + " row count : " + rowCount);
        for (Map.Entry<ColumnHandle, ColumnStatistics> entry : columnStatisticsMap.entrySet()) {
            System.out.println(olapTable.getName() + "." + ((DorisColumnHandle) entry.getKey()).getColumnName() + " : " + entry.getValue());
        }
        return new TableStatistics(Estimate.of(rowCount), columnStatisticsMap);
    }
}
