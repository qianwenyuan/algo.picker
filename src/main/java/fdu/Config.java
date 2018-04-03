package fdu;

public class Config {

    public static final String DEFAULT_POST_ADDRESS = "10.141.212.118";
    public static final String DEFAULT_DATAFEATUREMANAGER_ADDRESS = "10.132.140.232";
    public static final String RESULT_POST_PATH = ":1880/result";
    public static final String LOG_POST_PATH = ":1880/log";

    private static String defaultAddress = null;

    public static void setAddress(String address) {
        System.out.println("Setting address as " + address);
        if ("0:0:0:0:0:0:0:1".equals(address)) return;
        defaultAddress = address;
    }

    public static String getAddress() {
        if (defaultAddress == null)
            return DEFAULT_POST_ADDRESS;
        else return defaultAddress;
    }

    public static String getDFMAddress() {
        return DEFAULT_DATAFEATUREMANAGER_ADDRESS;
    }
}
