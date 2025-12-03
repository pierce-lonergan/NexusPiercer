package io.github.pierce.converter;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Accumulates conversion errors during record processing.
 *
 * <p>Implements the Notification pattern for collecting multiple errors
 * before failing, rather than fail-fast on the first error.</p>
 */
public class Notification {

    private final List<ConversionResult.ConversionError> errors = new ArrayList<>();
    private final Deque<String> fieldPath = new ArrayDeque<>();

    /**
     * Pushes a field name onto the path stack.
     * Returns an AutoCloseable that pops when closed.
     */
    public FieldContext pushField(String fieldName) {
        fieldPath.push(fieldName);
        return new FieldContext(this);
    }

    /**
     * Pops the current field from the path stack.
     */
    void popField() {
        if (!fieldPath.isEmpty()) {
            fieldPath.pop();
        }
    }

    /**
     * Returns the current field path as a dot-separated string.
     */
    public String getCurrentPath() {
        if (fieldPath.isEmpty()) {
            return "";
        }
        // Reverse the stack to get proper order
        List<String> pathList = new ArrayList<>(fieldPath);
        java.util.Collections.reverse(pathList);
        return String.join(".", pathList);
    }

    /**
     * Adds an error at the current field path.
     */
    public void addError(String message) {
        errors.add(new ConversionResult.ConversionError(getCurrentPath(), message, null));
    }

    /**
     * Adds an error with a cause at the current field path.
     */
    public void addError(String message, Throwable cause) {
        errors.add(new ConversionResult.ConversionError(getCurrentPath(), message, cause));
    }

    /**
     * Adds an error with explicit path.
     */
    public void addError(String fieldPath, String message, Throwable cause) {
        String fullPath = getCurrentPath();
        if (!fullPath.isEmpty() && fieldPath != null && !fieldPath.isEmpty()) {
            fullPath = fullPath + "." + fieldPath;
        } else if (fieldPath != null) {
            fullPath = fieldPath;
        }
        errors.add(new ConversionResult.ConversionError(fullPath, message, cause));
    }

    /**
     * Returns true if any errors have been recorded.
     */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    /**
     * Returns the list of all errors.
     */
    public List<ConversionResult.ConversionError> getErrors() {
        return new ArrayList<>(errors);
    }

    /**
     * Formats all errors into a single string.
     */
    public String formatErrors() {
        return errors.stream()
                .map(e -> {
                    String path = e.fieldPath();
                    return (path != null && !path.isEmpty() ? path + ": " : "") + e.message();
                })
                .collect(Collectors.joining("; "));
    }

    /**
     * Creates a ConversionResult from the current state.
     */
    public <T> ConversionResult<T> toResult(T value) {
        if (hasErrors()) {
            return ConversionResult.partial(value, getErrors());
        }
        return ConversionResult.success(value);
    }

    /**
     * Clears all errors.
     */
    public void clear() {
        errors.clear();
        fieldPath.clear();
    }

    /**
     * AutoCloseable context for field path management.
     */
    public static class FieldContext implements AutoCloseable {
        private final Notification notification;

        FieldContext(Notification notification) {
            this.notification = notification;
        }

        @Override
        public void close() {
            notification.popField();
        }
    }
}
