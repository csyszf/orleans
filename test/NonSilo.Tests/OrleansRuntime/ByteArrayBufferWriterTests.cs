using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using Orleans.Configuration;
using Orleans.Runtime;
using Xunit;

namespace NonSilo.Tests.OrleansRuntime
{
    public class ByteArrayBufferWriterTests
    {
        [Fact]
        public void BuildBuffer()
        {
            BufferPool.InitGlobalBufferPool(new SiloMessagingOptions { BufferPoolMinimumBufferSize = 1024});

            var rnd = new Random();
            var source = RandomBytes().Take(20).ToArray();

            var writer = new ByteArrayBufferWriter();
            foreach (var item in source)
            {
                var span = new Span<byte>(item);

                while (span.Length > 0)
                {
                    var buff = writer.GetSpan(item.Length);
                    var writeBytes = span.Length;
                    var toWrite = span.Slice(0, writeBytes);
                    toWrite.CopyTo(buff);
                    writer.Advance(writeBytes);
                    span = span.Slice(writeBytes);
                }
            }

            var buffer = writer.Buffer.ToArray().AsSpan();
            foreach (var item in source)
            {
                Assert.True(item.AsSpan().SequenceEqual(buffer.Slice(0, item.Length)));
                buffer = buffer.Slice(item.Length);
            }

            IEnumerable<byte[]> RandomBytes()
            {
                while (true)
                {
                    var length = rnd.Next(1, 4096);
                    var buff = new byte[length];
                    rnd.NextBytes(buff);
                    yield return buff;
                }
            }
        }
    }
}
