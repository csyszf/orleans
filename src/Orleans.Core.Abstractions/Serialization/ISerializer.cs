using System;

namespace Orleans.Serialization
{
    public interface ISerializer
    {
        object Deserializer(Type expected, IDeserializationContext context);
        void Serializer(object raw, ref BinaryTokenStreamWriter writer, Type expected);
        object DeepCopier(object original, ICopyContext context);
    }

    public interface IUntypedSerializer : ISerializer
    {
    }

    public interface ISerializer<T> : ISerializer
    {

    }
}
