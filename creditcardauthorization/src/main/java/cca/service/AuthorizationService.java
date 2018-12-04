package cca.service;

import cca.Order;

import java.io.Serializable;

public class AuthorizationService implements Serializable {
    public boolean authorize(Order order){
        // In a real scenario, this would call an external cca.service to verify the credit card number for the order
        return true;
    }
}
