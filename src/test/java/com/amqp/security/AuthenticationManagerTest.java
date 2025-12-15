package com.amqp.security;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

class AuthenticationManagerTest {

    private AuthenticationManager authManager;

    @BeforeEach
    void setUp() {
        // Enable guest user for testing
        authManager = new AuthenticationManager(true);
    }

    @Test
    void testDefaultGuestUserExists() {
        User guest = authManager.authenticate("guest", "guest");
        assertThat(guest).isNotNull();
        assertThat(guest.getUsername()).isEqualTo("guest");
        // Guest user has administrator permissions when enabled (for testing)
        // In production, guest should be disabled by default
        assertThat(guest.isAdministrator()).isTrue();
    }

    @Test
    void testDefaultVirtualHostExists() {
        VirtualHost vhost = authManager.getVirtualHost("/");
        assertThat(vhost).isNotNull();
        assertThat(vhost.getName()).isEqualTo("/");
        assertThat(vhost.isActive()).isTrue();
    }

    @Test
    void testAuthenticateValidUser() {
        Set<String> tags = new HashSet<>();
        tags.add("management");
        authManager.createUser("testuser", "testpass", tags);

        User user = authManager.authenticate("testuser", "testpass");
        assertThat(user).isNotNull();
        assertThat(user.getUsername()).isEqualTo("testuser");
        assertThat(user.isManagement()).isTrue();
    }

    @Test
    void testAuthenticateInvalidPassword() {
        authManager.createUser("testuser", "testpass", new HashSet<>());
        User user = authManager.authenticate("testuser", "wrongpass");
        assertThat(user).isNull();
    }

    @Test
    void testAuthenticateNonExistentUser() {
        User user = authManager.authenticate("nonexistent", "password");
        assertThat(user).isNull();
    }

    @Test
    void testCreateAndDeleteUser() {
        authManager.createUser("tempuser", "temppass", new HashSet<>());
        assertThat(authManager.getUser("tempuser")).isNotNull();

        authManager.deleteUser("tempuser");
        assertThat(authManager.getUser("tempuser")).isNull();
    }

    @Test
    void testCannotDeleteGuestUser() {
        assertThatThrownBy(() -> authManager.deleteUser("guest"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("guest");
    }

    @Test
    void testSetUserPermissions() {
        authManager.createUser("limiteduser", "pass", new HashSet<>());

        authManager.setUserPermissions("limiteduser", "/", "^test.*", "^test.*", "^test.*");

        User user = authManager.getUser("limiteduser");
        VirtualHostPermissions perms = user.getPermissions("/");

        assertThat(perms).isNotNull();
        assertThat(perms.canConfigure("test.queue")).isTrue();
        assertThat(perms.canConfigure("other.queue")).isFalse();
    }

    @Test
    void testAdministratorHasAllPermissions() {
        // Create a proper administrator user
        Set<String> adminTags = new HashSet<>();
        adminTags.add("administrator");
        authManager.createUser("admin", "adminpass", adminTags);
        User admin = authManager.getUser("admin");

        assertThat(authManager.authorize(admin, "/", Permission.CONFIGURE, "any.resource")).isTrue();
        assertThat(authManager.authorize(admin, "/", Permission.WRITE, "any.resource")).isTrue();
        assertThat(authManager.authorize(admin, "/", Permission.READ, "any.resource")).isTrue();
    }

    @Test
    void testCreateAndDeleteVirtualHost() {
        authManager.createVirtualHost("/test");
        VirtualHost vhost = authManager.getVirtualHost("/test");

        assertThat(vhost).isNotNull();
        assertThat(vhost.getName()).isEqualTo("/test");

        authManager.deleteVirtualHost("/test");
        assertThat(authManager.getVirtualHost("/test")).isNull();
    }

    @Test
    void testCannotDeleteDefaultVirtualHost() {
        assertThatThrownBy(() -> authManager.deleteVirtualHost("/"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("default");
    }

    @Test
    void testVirtualHostResourceLimits() {
        VirtualHost vhost = authManager.getVirtualHost("/");

        vhost.setMaxQueues(10);
        vhost.setMaxExchanges(20);
        vhost.setMaxConnections(100);

        assertThat(vhost.getMaxQueues()).isEqualTo(10);
        assertThat(vhost.getMaxExchanges()).isEqualTo(20);
        assertThat(vhost.getMaxConnections()).isEqualTo(100);
    }
}
