package org.learn.datalake.common;

public class DBConfig {
  public static class MYSQLJKDB {
    public static String userName = "jcuser";
    public static String passWd = "qOIBbe5sTOfB6oKxf";
    public static String host = "121.36.143.205";
    public static String dbName = "cdc";
    public static String tableName = "device_source";
    public static int port = 3306;
  }

  public static class MSJKDB {
    public static String userName = "JK";
    public static String passWd = "JK123";
    public static String host = "192.168.1.217";
    public static String dbName = "IPMS4S_HRXJ_JK";
    public static String tableName = "CAR_GPSDATA";
    public static int port = 1433;
  }

  public static class MSTestDB {
    public static String userName = "sa";
    public static String passWd = "Wm@12345";
    public static String host = "122.112.190.101";
    public static String dbName = "SampleDb";
    public static String tableName = "SampleTable";
    public static int port = 1433;
  }
}
