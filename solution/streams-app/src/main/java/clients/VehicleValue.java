package clients;

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
    public float long$;
    
    // due to the fact that "long" is a reserved word in Java
    // we have to implement the attribute as a property with
    // getter and setter!
    private float longitude;
    public void setLong(float longitude){
        this.longitude = longitude;
    }
    public float getLong(){
        return this.longitude;
    }

    public float acc;
    public int dl;
    public int odo;
    public int drst;
    public String oday;
    public int jrn;
    public int line;
    public String start;
}
