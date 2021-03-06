package cca.dao;

import cca.Order;

import java.io.Serializable;

public class OrderDao implements Serializable {
    public boolean isNotReadyToShip(Order order) {
        // In a real scenario, this would check the status of the order in the DB.
        return true;
    }

    public void updateStatusToReadyToShip(Order order) {
        // In a real scenario, this would update the status of the order in the DB.
    }

    public void updateStatusToDenied(Order order) {
        // In a real scenario, this would update the status of the order in the DB.
    }
    public boolean isSystemAvailable(){
        //check to see if the database is available
        return true;
    }
}