package com.amqp.protocol.v10.types;

/**
 * AMQP 1.0 Described Type.
 * A described type wraps another type with a descriptor that identifies the semantics.
 */
public interface DescribedType {

    /**
     * Get the descriptor that identifies this type.
     * Can be a Symbol or a ULong.
     */
    Object getDescriptor();

    /**
     * Get the described value.
     */
    Object getDescribed();

    /**
     * Simple implementation of DescribedType.
     */
    class Default implements DescribedType {
        private final Object descriptor;
        private final Object described;

        public Default(Object descriptor, Object described) {
            this.descriptor = descriptor;
            this.described = described;
        }

        @Override
        public Object getDescriptor() {
            return descriptor;
        }

        @Override
        public Object getDescribed() {
            return described;
        }

        @Override
        public String toString() {
            return "Described{descriptor=" + descriptor + ", described=" + described + "}";
        }
    }
}
