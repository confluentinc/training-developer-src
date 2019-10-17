package clients;

import com.fasterxml.jackson.annotation.JsonProperty;

public class VehicleValue {
    public String desi;
    public String dir;
    public int oper;
    public int veh;
    public String tst;
    public int tsi;
    public float spd;
    public int hdg;
    public float lat;

    @JsonProperty("long")
    public float longitude;

    public float acc;
    public int dl;
    public int odo;
    public int drst;
    public String oday;
    public int jrn;
    public int line;
    public String start;
    public String loc;
    public String stop;
    public String route;
    public int occu;
    public int seq;
}
