package alex.RTBCEP;
import com.google.gson.annotations.SerializedName;

public  class Event {

    @SerializedName("campaign_id")
    private String campaign_id;

    @SerializedName("ad_id")
    private String ad_id;

    @SerializedName("timestamp")
    private Float timestamp;

    @SerializedName("country")
    private String country;

    @SerializedName("os")
    private String os;

    @SerializedName("dev_id")
    private String dev_id;

    @SerializedName("model")
    private String model;

    @SerializedName("age")
    private int age;

    @SerializedName("gender")
    private String gender;

    @SerializedName("pub_id")
    private String pub_id;

    @SerializedName("type")
    private String type;

    public Event(String campaign_id, String ad_id, Float timestamp, String country, String os, String dev_id, String model, int age, String gender, String pub_id, String type) {
        this.campaign_id = campaign_id;
        this.ad_id = ad_id;
        this.timestamp = timestamp;
        this.country = country;
        this.os = os;
        this.dev_id = dev_id;
        this.model = model;
        this.age = age;
        this.gender = gender;
        this.pub_id = pub_id;
        this.type = type;
    }


    public String toString(){
        return new String("Ad_id: " + getAd_id());
    }

    public String getCampaign_id() {
        return campaign_id;
    }

    public String getAd_id() {
        return ad_id;
    }

    public Float getTimestamp() {
        return timestamp;
    }

    public String getCountry() {
        return country;
    }

    public String getOs() {
        return os;
    }

    public String getDev_id() {
        return dev_id;
    }

    public String getModel() {
        return model;
    }

    public int getAge() {
        return age;
    }

    public String getGender() {
        return gender;
    }

    public String getPub_id() {
        return pub_id;
    }

    public String getType() {
        return type;
    }
}

