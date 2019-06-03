package clients;

public class DoorStatusChanged{
    public DoorStatusChanged(String operator, String designation, String vehicleNo, String type){
        this.operator = operator;
        this.route = designation;
        this.vehicleNo = vehicleNo;
        this.type = type;
    }

    public String operator;
    public String route;
    public String vehicleNo;
    public String type;
}