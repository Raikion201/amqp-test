package com.amqp.protocol.v10.security;

/**
 * Authorization provider interface for AMQP 1.0.
 *
 * Implementations decide whether authenticated users are allowed
 * to perform specific operations on resources.
 */
public interface Amqp10AuthorizationProvider {

    /**
     * Resource types that can be authorized.
     */
    enum ResourceType {
        QUEUE,
        EXCHANGE,
        VHOST,
        CONNECTION,
        TRANSACTION
    }

    /**
     * Operations that can be authorized.
     */
    enum Operation {
        // Queue operations
        CONSUME,
        PUBLISH,
        CREATE,
        DELETE,
        BIND,
        UNBIND,
        PURGE,

        // Connection operations
        CONNECT,
        DISCONNECT,

        // Transaction operations
        DECLARE_TRANSACTION,
        COMMIT_TRANSACTION,
        ROLLBACK_TRANSACTION,

        // Admin operations
        CONFIGURE,
        MANAGE
    }

    /**
     * Check if the user is authorized to perform an operation on a resource.
     *
     * @param username the authenticated username
     * @param vhost the virtual host (null for connection-level checks)
     * @param resourceType the type of resource
     * @param resourceName the name of the resource (queue/exchange name)
     * @param operation the operation being attempted
     * @return true if authorized, false otherwise
     */
    boolean authorize(String username, String vhost, ResourceType resourceType,
                      String resourceName, Operation operation);

    /**
     * Check if the user can connect to the specified virtual host.
     */
    default boolean canConnect(String username, String vhost) {
        return authorize(username, vhost, ResourceType.VHOST, vhost, Operation.CONNECT);
    }

    /**
     * Check if the user can consume from a queue.
     */
    default boolean canConsume(String username, String vhost, String queueName) {
        return authorize(username, vhost, ResourceType.QUEUE, queueName, Operation.CONSUME);
    }

    /**
     * Check if the user can publish to a queue or exchange.
     */
    default boolean canPublish(String username, String vhost, String destination) {
        // Try both exchange and queue
        return authorize(username, vhost, ResourceType.EXCHANGE, destination, Operation.PUBLISH)
                || authorize(username, vhost, ResourceType.QUEUE, destination, Operation.PUBLISH);
    }

    /**
     * Check if the user can create a resource.
     */
    default boolean canCreate(String username, String vhost, ResourceType type, String name) {
        return authorize(username, vhost, type, name, Operation.CREATE);
    }

    /**
     * Check if the user can delete a resource.
     */
    default boolean canDelete(String username, String vhost, ResourceType type, String name) {
        return authorize(username, vhost, type, name, Operation.DELETE);
    }

    /**
     * Default implementation that allows everything.
     * USE ONLY FOR DEVELOPMENT/TESTING.
     */
    static Amqp10AuthorizationProvider allowAll() {
        return (username, vhost, resourceType, resourceName, operation) -> true;
    }

    /**
     * Default implementation that denies everything except connection.
     */
    static Amqp10AuthorizationProvider denyAll() {
        return (username, vhost, resourceType, resourceName, operation) ->
                operation == Operation.CONNECT;
    }

    /**
     * Create a simple role-based authorization provider.
     */
    static Amqp10AuthorizationProvider roleBasedProvider(RoleProvider roleProvider) {
        return new RoleBasedAuthorizationProvider(roleProvider);
    }

    /**
     * Interface for providing user roles.
     */
    interface RoleProvider {
        /**
         * Get roles for a user.
         */
        java.util.Set<String> getRoles(String username);
    }

    /**
     * Simple role-based authorization provider.
     */
    class RoleBasedAuthorizationProvider implements Amqp10AuthorizationProvider {
        private final RoleProvider roleProvider;

        public RoleBasedAuthorizationProvider(RoleProvider roleProvider) {
            this.roleProvider = roleProvider;
        }

        @Override
        public boolean authorize(String username, String vhost, ResourceType resourceType,
                                 String resourceName, Operation operation) {
            java.util.Set<String> roles = roleProvider.getRoles(username);
            if (roles == null || roles.isEmpty()) {
                return false;
            }

            // Admin role can do everything
            if (roles.contains("admin") || roles.contains("administrator")) {
                return true;
            }

            // Check specific role-based permissions
            switch (operation) {
                case CONNECT:
                case CONSUME:
                case PUBLISH:
                    return roles.contains("user") || roles.contains("management");

                case CREATE:
                case DELETE:
                case BIND:
                case UNBIND:
                case PURGE:
                    return roles.contains("management") || roles.contains("policymaker");

                case CONFIGURE:
                case MANAGE:
                    return roles.contains("management");

                case DECLARE_TRANSACTION:
                case COMMIT_TRANSACTION:
                case ROLLBACK_TRANSACTION:
                    return roles.contains("user") || roles.contains("management");

                default:
                    return false;
            }
        }
    }
}
