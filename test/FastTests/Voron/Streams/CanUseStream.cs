﻿using System;
using System.IO;
using Xunit;

namespace FastTests.Voron.Streams
{
    public class CanUseStream : StorageTest
    {
        [Theory]
        [InlineData(129)]
        [InlineData(2095)]
        [InlineData(4096)]
        [InlineData(12004)]
        [InlineData(15911)]
        [InlineData(31911)]
        [InlineData(91911)]
        public void CanWriteAndRead(int size)
        {
            var buffer = new byte[size];
            new Random().NextBytes(buffer);
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Files");
                tree.AddStream("test", new MemoryStream(buffer));
                tx.Commit();
            }
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("Files");
                var readStream = tree.ReadStream("test");
                Assert.NotNull(readStream);
                for (int i = 0; i < buffer.Length; i++)
                {
                    Assert.Equal(buffer[i], readStream.ReadByte());
                }
                Assert.Equal(-1, readStream.ReadByte());
            }
        }

        [Fact]
        public void CanUpdate()
        {
            var buffer = new byte[50];
            new Random().NextBytes(buffer);
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Files");
                tree.AddStream("test", new MemoryStream(buffer));
                tx.Commit();
            }
            new Random().NextBytes(buffer);
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Files");
                tree.AddStream("test", new MemoryStream(buffer));
                tx.Commit();
            }
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("Files");
                var readStream = tree.ReadStream("test");
                Assert.NotNull(readStream);
                for (int i = 0; i < buffer.Length; i++)
                {
                    Assert.Equal(buffer[i], readStream.ReadByte());
                }
                Assert.Equal(-1, readStream.ReadByte());
            }
        }
    }
}