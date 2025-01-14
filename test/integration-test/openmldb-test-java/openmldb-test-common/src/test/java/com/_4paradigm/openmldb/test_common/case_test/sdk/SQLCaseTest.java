/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.test_common.case_test.sdk;


import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.provider.Yaml;
import org.testng.Assert;

import org.testng.annotations.Test;

import com._4paradigm.openmldb.batch.api.OpenmldbSession;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import java.util.List;
import java.util.ArrayList;

import com._4paradigm.openmldb.test_common.common.BaseTest;
import com._4paradigm.openmldb.test_common.util.RowsSort;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com._4paradigm.openmldb.test_common.provider.YamlUtil;
import com._4paradigm.openmldb.test_common.util.Tool;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import org.testng.annotations.AfterTest;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SQLCaseTest {
    protected OpenMLDBInfo openMLDBInfo= YamlUtil.getObject(Tool.openMLDBDir().getAbsolutePath()+"/out/openmldb_info.yaml",OpenMLDBInfo.class);
    protected String defaultDb = "db1";
    public SparkSession ss = SparkSession
    .builder()
    .master("local[4]")
    .config("openmldb.zk.cluster", "node-4:22172")
    .config("openmldb.zk.root.path","/openmldb-unionall-0.8.3-11301637")
    .config("openmldb.default.db",defaultDb)
    .getOrCreate();
    public OpenmldbSession om = new OpenmldbSession(ss);
    

    @Test(dataProvider = "getCase",dataProviderClass = BaseTest.class)
    //@Yaml(filePaths = "integration_test/tmp/")
   //@Yaml(filePaths = "integration_test/expression/")
   //@Yaml(filePaths = "integration_test/function/")
   //@Yaml(filePaths = "integration_test/cluster/")
   //@Yaml(filePaths = "integration_test/join/")
    //@Yaml(filePaths = "integration_test/select/")
   // @Yaml(filePaths = "integration_test/test_index_optimized.yaml")
    @Yaml(filePaths = "integration_test/long_window/test_xxx_where.yaml") //longWindow: w1:2s参数怎么放进去
    //@Yaml(filePaths = "integration_test/multiple_databases/")
    public void testSqlFormat(SQLCase sqlCase) {
        log.info(sqlCase.getDesc());
        if(null != sqlCase.getMode() && sqlCase.getMode().contains("request-unsupport")){
            return ;
        }
        if(null != sqlCase.getMode() && sqlCase.getMode().contains("cluster-unsupport")){
            return;
        }
        if(null != sqlCase.getMode() && sqlCase.getMode().contains("hybridse-only")){
            return ;
        }
        if(null != sqlCase.getMode() && sqlCase.getMode().contains("batch-unsupport")){
            return ;
        }
        if(null != sqlCase.getMode() && sqlCase.getMode().contains("rtidb-unsupport")){
            return ;
        }
        if(null != sqlCase.getMode() && sqlCase.getMode().contains("offline-unsupport")){
            return ;
        }
        List<InputDesc> tables = sqlCase.getInputs();
        om.disableSparkLogs();
        for (InputDesc table :tables){
            Dataset<Row> inputdf = WriteToSpark(table.getRows(),table.getColumns(),table.getName(),sqlCase);
            om.registerTable(defaultDb, table.getName(), inputdf);
            om.registerTableInOpenmldbSession(defaultDb, table.getName(), inputdf);
         //   SparkUtil. .addIndexColumn(ss, inputdf, defaultDb, defaultDb)
        }       
        List<Row> realRow = null;
        try{
            Dataset<Row> df1 = om.openmldbSql(sqlCase.getSql()).sparkDf();
        //    if(sqlCase.getExpect().getOrder()!=null){df1.orderBy(sqlCase.getExpect().getOrder());};
            realRow = df1.collectAsList();
        } catch (Exception e) {
            log.info(sqlCase.getDesc()+e.toString());
        }
        Dataset<Row> outputdf = WriteToSpark(sqlCase.getExpect().getRows(),sqlCase.getExpect().getColumns(),"output",sqlCase);
        //if(sqlCase.getExpect().getOrder()!=null){outputdf.orderBy(sqlCase.getExpect().getOrder());};
        List<Row> expectRow = outputdf.collectAsList();
        if (!sqlCase.getExpect().getSuccess()){ Assert.assertNull(realRow,sqlCase.getDesc()+" is not null");}
        else {
            Compare(realRow,expectRow,sqlCase);
        }
            
    }

    @AfterTest
    public void tearDown(){
        om.stop();
        ss.stop();
        om.close();
        ss.close();
    }
    public void Compare(List<Row> realRow, List<Row> expectRow,SQLCase sqlCase){
        if (null==realRow){
            Assert.assertNull(expectRow,sqlCase.getDesc()+" is not null");
            return;
        }
        try{
            realRow.sort(new RowsSort(0));
            expectRow.sort(new RowsSort(0));
        } catch (Exception e){
            log.error( sqlCase.getDesc()+" sort error "+ e);
        }
        Assert.assertEquals(realRow.size(), expectRow.size());
        for(int i=0;i<expectRow.size();i++){
            for(int j=0;j<expectRow.get(i).length();j++){
                if (expectRow.get(i).apply(j)==null||realRow.get(i).apply(j)==null){
                    Assert.assertNull(realRow.get(i).apply(j), sqlCase.getDesc()+" is not null");
                    Assert.assertNull(expectRow.get(i).apply(j), sqlCase.getDesc()+" is not null");
                } else if (expectRow.get(i).apply(j) instanceof Float ){
                    Assert.assertEquals((float)realRow.get(i).apply(j),(float)expectRow.get(i).apply(j),1e-4,
                    sqlCase.getDesc()+"error key value is "+ j);
                } else if (expectRow.get(i).apply(j) instanceof Double){
                    Assert.assertEquals((double)realRow.get(i).apply(j),(double)expectRow.get(i).apply(j),1e-4,
                    sqlCase.getDesc()+"error key value is "+ j);
                } 
                 else {
                    Assert.assertEquals(realRow.get(i).apply(j).toString(),expectRow.get(i).apply(j).toString(),
                    sqlCase.getDesc()+"error key value is "+ j);
                }
            }

        }
        return;
    }


    public Dataset<Row> WriteToSpark(List<List<Object>> rows, List<String> schemas,String tableName,SQLCase sqlCase){
        List<StructField> schemaList = new ArrayList<StructField>();
        StructType schema = null;
        List<Row> data = new ArrayList<Row>();
        Dataset<Row> df;
        schemas.forEach(nametype->{
            String name = "";
            String type = "";
            Boolean nullable = true;
            String[] nameType = nametype.split(" ");
            int notnull = 0;
            if (nameType.length>=4&&nameType[nameType.length-1].equals("null")){
                notnull = 2;
                nullable = false;
            }
            for (int k=0; k<nameType.length-notnull;k++){
                if (k == nameType.length-1-notnull) 
                {   
                    type = switchType(nameType[k]);break;
                }
                name = name + nameType[k];
            }
            try{switch (type) {
                case "int":
                    schemaList.add(DataTypes.createStructField(name, DataTypes.IntegerType, nullable));
                    break;
                case "bool":
                    schemaList.add(DataTypes.createStructField(name, DataTypes.BooleanType, nullable));
                    break;
                case "string":
                    schemaList.add(DataTypes.createStructField(name, DataTypes.StringType, nullable));
                    break;
                case "double":
                    schemaList.add(DataTypes.createStructField(name, DataTypes.DoubleType, nullable));
                    break;
                case "long":
                    schemaList.add(DataTypes.createStructField(name, DataTypes.LongType, nullable));
                    break;
                case "float":
                    schemaList.add(DataTypes.createStructField(name, DataTypes.FloatType, nullable));
                    break;
                case "date":
                    schemaList.add(DataTypes.createStructField(name, DataTypes.DateType, nullable));
                    break;
                case "timestamp":
                    schemaList.add(DataTypes.createStructField(name, DataTypes.TimestampType, nullable));
                    break;
                default:
                    System.out.println("a");
                    break;
            }
 } 
            catch(Exception e){
                log.error( sqlCase.getDesc()+" sort error "+ e);
            }
            
        });
        try{schema = DataTypes.createStructType(schemaList);}
        catch(Exception e){
            log.error( sqlCase.getDesc()+" sort error "+ e);
        }
        
                rows.forEach(row->{    
                    Object[] os = new Object[row.size()];
                    
                    for(int i=0;i<row.size();i++){                       
                        Object o = row.get(i);
                        Object d = null;
                        String[] nameType = schemas.get(i).split(" ");
                        String type = "";
                        int notnull = 0;
                        if (nameType.length>=4&&nameType[nameType.length-1].equals("null")){
                            notnull = 2;
                        }
                        type = nameType[nameType.length-1-notnull];
                        
                        if (o==null){
                            os[i] = null;
                            continue;
                        }
                        switch (switchType(type)) {
                            case "int":
                                o = Double.parseDouble(o.toString());
                                d = (new Double((double)o)).intValue(); break;
                            case "double":
                                d = Double.parseDouble(o.toString());break;               
                            case "bool":
                                d = (Boolean)o;break;
                            case "string":
                                d = (String)o;break;
                            case "long":
                                o = Double.parseDouble(o.toString());
                                d = (new Double((double)o)).longValue(); break;
                            case "float":
                                o = Double.parseDouble(o.toString());
                                d = (new Double((double)o)).floatValue(); break;        
                            case "timestamp":
                                o = Double.parseDouble(o.toString());
                                long ts = (new Double((double)o)).longValue();
                                d = new java.sql.Timestamp(ts);break;
                            case "date":
                               DateFormat fmt =new SimpleDateFormat("yyyy-MM-dd");
                               try {
                                long tss = fmt.parse(o.toString()).getTime();
                                d = new java.sql.Date(tss);
                               } catch (Exception e) {
                                System.out.println("parse date fail");
                       }
                        break;
                    default:
                        System.out.println("type unkown "+type.toString());
                        d = (String)o;
                }
                        os[i] = d;
                    }
                    Row r = RowFactory.create(os);
                    data.add(r);
                });
        df = ss.createDataFrame(data,schema);
        return df;   
    }

    public static String switchType(String type){
        switch(type){
            case "smallint":
                return "int";
            case "int":
                return "int";
            case "bigint":
                return "long";
            case "int32":
                return "int";
            case "int16":
                return "int";
            case "int64":
                 return "long";
            case "float":
                return "float";               
            default:
                return type;
        }

    }

}
