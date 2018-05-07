package io.appery.dbdates;

class ApperyDbException extends RuntimeException {

    String reason;

    ApperyDbException(String reason) {
        this.reason = reason;
    }
}
