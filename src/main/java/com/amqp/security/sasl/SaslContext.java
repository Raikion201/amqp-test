package com.amqp.security.sasl;

import io.netty.channel.ChannelHandlerContext;

import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

/**
 * Context for SASL authentication.
 *
 * Contains information about the connection and authentication state.
 */
public class SaslContext {

    private final ChannelHandlerContext channelContext;

    // Connection info
    private String remoteAddress;
    private int remotePort;

    // TLS info
    private boolean tlsEnabled;
    private X509Certificate clientCertificate;
    private String tlsUsername;

    // Authentication state
    private String selectedMechanism;
    private String authenticatedUser;
    private boolean authenticated;

    // SCRAM state
    private byte[] scramState;
    private String scramNonce;

    // Arbitrary state storage
    private final Map<String, Object> attributes = new HashMap<>();

    public SaslContext(ChannelHandlerContext channelContext) {
        this.channelContext = channelContext;
        if (channelContext != null && channelContext.channel() != null) {
            java.net.SocketAddress addr = channelContext.channel().remoteAddress();
            if (addr instanceof java.net.InetSocketAddress) {
                java.net.InetSocketAddress inet = (java.net.InetSocketAddress) addr;
                this.remoteAddress = inet.getAddress().getHostAddress();
                this.remotePort = inet.getPort();
            }
        }
    }

    // Connection info
    public ChannelHandlerContext getChannelContext() {
        return channelContext;
    }

    public String getRemoteAddress() {
        return remoteAddress;
    }

    public int getRemotePort() {
        return remotePort;
    }

    // TLS info
    public boolean isTlsEnabled() {
        return tlsEnabled;
    }

    public void setTlsEnabled(boolean tlsEnabled) {
        this.tlsEnabled = tlsEnabled;
    }

    public X509Certificate getClientCertificate() {
        return clientCertificate;
    }

    public void setClientCertificate(X509Certificate clientCertificate) {
        this.clientCertificate = clientCertificate;
    }

    public String getTlsUsername() {
        return tlsUsername;
    }

    public void setTlsUsername(String tlsUsername) {
        this.tlsUsername = tlsUsername;
    }

    public boolean hasClientCertificate() {
        return clientCertificate != null;
    }

    // Authentication state
    public String getSelectedMechanism() {
        return selectedMechanism;
    }

    public void setSelectedMechanism(String selectedMechanism) {
        this.selectedMechanism = selectedMechanism;
    }

    public String getAuthenticatedUser() {
        return authenticatedUser;
    }

    public void setAuthenticatedUser(String authenticatedUser) {
        this.authenticatedUser = authenticatedUser;
        this.authenticated = authenticatedUser != null;
    }

    public boolean isAuthenticated() {
        return authenticated;
    }

    // SCRAM state
    public byte[] getScramState() {
        return scramState;
    }

    public void setScramState(byte[] scramState) {
        this.scramState = scramState;
    }

    public String getScramNonce() {
        return scramNonce;
    }

    public void setScramNonce(String scramNonce) {
        this.scramNonce = scramNonce;
    }

    // Arbitrary attributes
    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key, Class<T> type) {
        Object value = attributes.get(key);
        if (type.isInstance(value)) {
            return (T) value;
        }
        return null;
    }

    public void removeAttribute(String key) {
        attributes.remove(key);
    }

    public void clearAttributes() {
        attributes.clear();
    }

    @Override
    public String toString() {
        return String.format("SaslContext{remote=%s:%d, tls=%s, mechanism=%s, authenticated=%s, user=%s}",
                remoteAddress, remotePort, tlsEnabled, selectedMechanism, authenticated, authenticatedUser);
    }
}
