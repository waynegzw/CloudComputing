package com.cloudcomputing.samza.pitt_cabs;

/**
 * Created by Zhangwei on 4/19/16.
 */
public class Driver {
    private int driverId;
    private int blockId;
    private int latitude;
    private int longitude;
    private String gender;
    private String status;
    private double rating;
    private int salary;

    public Driver(int driverid, int blockid, int latitude, int longitude, String status) {
        this.driverId = driverid;
        this.blockId = blockid;
        this.latitude = latitude;
        this.longitude = longitude;
        this.status = status;
    }

    public Driver(int driverid, int blockid, int latitude, int longitude, String status, String gender, double rating, int salary) {
        this.driverId = driverid;
        this.blockId = blockid;
        this.latitude = latitude;
        this.longitude = longitude;
        this.gender = gender;
        this.status = status;
        this.rating = rating;
        this.salary = salary;
    }

    public int getDriverId() {
        return driverId;
    }

    public void setDriverId(int driverid) {
        this.driverId = driverid;
    }

    public int getBlockId() {
        return blockId;
    }

    public void setBlockId(int blockid) {
        this.blockId = blockid;
    }

    public int getLatitude() {
        return latitude;
    }

    public void setLatitude(int latitude) {
        this.latitude = latitude;
    }

    public int getLongitude() {
        return longitude;
    }

    public void setLongitude(int longitude) {
        this.longitude = longitude;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }
}
