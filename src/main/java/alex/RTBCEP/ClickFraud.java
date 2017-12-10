package alex.RTBCEP;

public class ClickFraud {

    private String DevId;

    public ClickFraud(String DevId) {
        this.DevId = DevId;
    }

    public ClickFraud() {
        this("");
    }

    public String getDevId() {
        return DevId;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ClickFraud) {
            ClickFraud other = (ClickFraud) obj;
            return DevId == other.DevId;
        } else {
            return false;
        }
    }


    @Override
    public String toString() {
        return "Click Fraud: { DevId : " + getDevId() + " }";
    }

}
