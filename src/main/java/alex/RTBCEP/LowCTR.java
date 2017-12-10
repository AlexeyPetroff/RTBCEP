package alex.RTBCEP;

public class LowCTR {

    private String AdId;

    public LowCTR(String AdId) {
        this.AdId = AdId;
    }

    public LowCTR() {
        this("");
    }

    public String getAdId() {
        return AdId;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LowCTR) {
            LowCTR other = (LowCTR) obj;
            return AdId == other.AdId;
        } else {
            return false;
        }
    }


    @Override
    public String toString() {
        return "LOW CTR: { AdId : " + getAdId() + " }";
    }

}
