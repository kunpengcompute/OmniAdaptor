/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.apache.flink.table.planner.plan.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

/**
 * ReflectionUtils
 *
 * @since 2025-04-27
 */
public class ReflectionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ReflectionUtils.class);

    /**
     * retrievePrivateField
     *
     * @param obj obj
     * @param fieldName fieldName
     * @return T
     */
    public static <T> T retrievePrivateField(Object obj, String fieldName) {
        try {
            Class<?> clazz = obj.getClass();
            Field field = null;

            // Search for the field in the class hierarchy
            while (clazz != null) {
                try {
                    field = clazz.getDeclaredField(fieldName);
                    break; // Field found, exit the loop
                } catch (NoSuchFieldException e) {
                    clazz = clazz.getSuperclass(); // Check the superclass
                }
            }

            if (field == null) {
                throw new RuntimeException("Field '"
                        + fieldName
                        + "' not found in class hierarchy of "
                        + obj.getClass().getName());
            }

            field.setAccessible(true); // Make the field accessible

            @SuppressWarnings("unchecked") // Cast is safe because of the generic type T
            T value = (T) field.get(obj);

            return value;
        } catch (IllegalAccessException | IllegalArgumentException e) {
            LOG.warn("Error retrieving private field: " + e.getMessage());
            throw new RuntimeException("Error retrieving private field: " + e.getMessage(), e);
        }
    }


    public static void main(String[] args) {
        // Example usage:
        class Parent {
            private String parentField = "Parent Value";

            public String getParentField() {
                return parentField;
            }
        }

        class Child extends Parent {
            private int childField = 42;

            public int getChildField() {
                return childField;
            }
        }

        Child child = new Child();

        String parentFieldValue = ReflectionUtils.retrievePrivateField(child, "parentField");
        int childFieldValue = ReflectionUtils.retrievePrivateField(child, "childField");

        LOG.info("Parent Field Value: {}", parentFieldValue);  // Output: Parent Value
        LOG.info("Child Field Value: {}", childFieldValue);    // Output: 42

        // Example of a non-existent field to show exception handling:
        try {
            String nonExistentField = ReflectionUtils.retrievePrivateField(child, "nonExistentField");
        } catch (RuntimeException e) {
            LOG.error("Caught RuntimeException: {}", e.getMessage()); // Correctly catches and logs
        }

        Parent parent = new Parent();
        String parentFieldValue2 = ReflectionUtils.retrievePrivateField(parent, "parentField");
        LOG.info("Parent Field Value (parent instance): {}", parentFieldValue2);  // Output: Parent Value
    }
}


