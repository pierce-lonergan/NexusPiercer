package io.github.pierce.converter;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Converter for list/array values.
 *
 * <p>Converts collections and arrays to List with elements converted
 * according to the element schema.</p>
 */
public class ListConverter extends AbstractTypeConverter<List<Object>> {

    private final TypeConverter<Object, ?> elementConverter;

    @SuppressWarnings("unchecked")
    public ListConverter(ConversionConfig config, TypeConverter<?, ?> elementConverter) {
        super(config, "list<" + elementConverter.getTargetTypeName() + ">");
        this.elementConverter = (TypeConverter<Object, ?>) Objects.requireNonNull(elementConverter, "Element converter cannot be null");
    }

    public TypeConverter<Object, ?> getElementConverter() {
        return elementConverter;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected List<Object> doConvert(Object value) throws TypeConversionException {
        Collection<?> source;

        // Handle List
        if (value instanceof List<?> list) {
            source = list;
        }
        // Handle other Collections
        else if (value instanceof Collection<?> coll) {
            source = coll;
        }
        // Handle Iterable
        else if (value instanceof Iterable<?> iter) {
            List<Object> list = new ArrayList<>();
            iter.forEach(list::add);
            source = list;
        }
        // Handle arrays
        else if (value.getClass().isArray()) {
            source = arrayToList(value);
        }
        else {
            throw unsupportedType(value);
        }

        // Convert elements
        List<Object> result = new ArrayList<>(source.size());
        int index = 0;

        for (Object element : source) {
            try {
                Object converted = elementConverter.convert(element);
                result.add(converted);
            } catch (TypeConversionException e) {
                String path = "[" + index + "]";
                if (e.getFieldPath() != null && !e.getFieldPath().isEmpty()) {
                    path = path + "." + e.getFieldPath();
                }
                throw new TypeConversionException(path, element,
                        elementConverter.getTargetTypeName(), e.getMessage(), e);
            }
            index++;
        }

        return result;
    }

    private List<?> arrayToList(Object array) {
        int length = Array.getLength(array);
        List<Object> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            list.add(Array.get(array, i));
        }
        return list;
    }

    @Override
    protected List<Object> handleNull() {
        return null;
    }
}