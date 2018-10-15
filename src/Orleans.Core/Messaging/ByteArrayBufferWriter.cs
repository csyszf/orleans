using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Orleans.Runtime
{
    internal class ByteArrayBufferWriter: IBufferWriter<byte>
    {
        private readonly BufferPool _pool;

        private long _currentWriteLength;

        private BufferSegment _writingHead;
        private BufferSegment _readHead;

        internal long Length => _currentWriteLength;
        internal ReadOnlySequence<byte> Buffer
        {
            get
            {
                BufferSegment head = _readHead;
                BufferSegment tail = _writingHead;
                return new ReadOnlySequence<byte>(head, 0, tail, tail.End);
            }
        }

        public ByteArrayBufferWriter()
        {
            _pool = BufferPool.GlobalPool;
        }

        public void ReleaseBuffers()
        {
            BufferSegment segment = _readHead;
            while (segment != null)
            {
                BufferSegment returnSegment = segment;
                segment = segment.NextSegment;

                returnSegment.ResetMemory();
            }
        }

        public Memory<byte> GetMemory(int sizeHint)
        {
            if (sizeHint < 0)
                throw new ArgumentOutOfRangeException(nameof(sizeHint));

            AllocateWriteHead(sizeHint);

            // Slice the AvailableMemory to the WritableBytes size
            int end = _writingHead.End;
            Memory<byte> availableMemory = _writingHead.AvailableMemory;
            availableMemory = availableMemory.Slice(end);
            return availableMemory;
        }

        public Span<byte> GetSpan(int sizeHint)
        {
            if (sizeHint < 0)
                throw new ArgumentOutOfRangeException(nameof(sizeHint));

            AllocateWriteHead(sizeHint);

            int end = _writingHead.End;
            Span<byte> availableSpan = _writingHead.AvailableMemory.Span;
            availableSpan = availableSpan.Slice(end);
            return availableSpan;
        }

        private void AllocateWriteHead(int sizeHint)
        {
            BufferSegment segment = null;

            if (_writingHead != null)
            {
                segment = _writingHead;

                int bytesLeftInBuffer = segment.WritableBytes;

                // If inadequate bytes left or if the segment is readonly
                if (bytesLeftInBuffer == 0 || bytesLeftInBuffer < sizeHint || segment.ReadOnly)
                {
                    BufferSegment nextSegment = new BufferSegment();
                    nextSegment.SetMemory(_pool.Rent(GetSegmentSize(sizeHint)));
                    segment.SetNext(nextSegment);
                    _writingHead = nextSegment;
                }
            }
            else
            {
                segment = new BufferSegment();
                segment.SetMemory(_pool.Rent(GetSegmentSize(sizeHint)));
                _writingHead = segment;
                _readHead = segment;
            }
        }

        // The size(adjustedToMaximumSize) of the buffer we get, possibly is less
        // than the number we applied(sizeHint), so we need to prepare
        // writing data to a buffer more than one time, although we set MaxBufferSize to int.MaxValue for now.
        // Or we could let The MemoryPool throw an exception about the outnumber sizeHint?
        private int GetSegmentSize(int sizeHint)
        {
            // First we need to handle case where hint is smaller than minimum segment size
            sizeHint = Math.Max(_pool.MinimumSize, sizeHint);
            // After that adjust it to fit into pools max buffer size
            var adjustedToMaximumSize = Math.Min(_pool.MaxBufferSize, sizeHint);
            return adjustedToMaximumSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Advance(int bytesWritten)
        {
            Memory<byte> buffer = _writingHead.AvailableMemory;

            // if bytesWritten is zero, these do nothing
            _writingHead.End += bytesWritten;
            _currentWriteLength += bytesWritten;
        }

    }
}
