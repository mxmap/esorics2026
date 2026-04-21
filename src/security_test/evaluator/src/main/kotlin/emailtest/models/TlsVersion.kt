package emailtest.models

// WATCH OUT: ORDER IS IMPORTANT FOR EVALUATION
enum class TlsVersion(val version: String) {
    NO_STARTTLS("no_starttls"),
    SSLV2("ssl2"),
    SSLV3("ssl3"),
    TLS1("tls1"),
    TLS1_1("tls1_1"),
    TLS1_2("tls1_2"),
    TLS1_3("tls1_3");
}


