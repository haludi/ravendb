﻿using System;
using System.Collections.Generic;
using System.Threading;
using Sparrow.Threading;

namespace Sparrow.Utils
{
    public class NativeMemoryCleaner<TStack, TPooledItem> : IDisposable where TPooledItem : PooledItem where TStack : StackHeader<TPooledItem>
    {
        private readonly object _lock = new object();
        private readonly Func<ICollection<TStack>> _getContexts;
        private readonly SharedMultipleUseFlag _lowMemoryFlag;
        private readonly TimeSpan _idleTime;
        private readonly Timer _timer;
        private WeakReference _selfweakref;

#if NETSTANDARD1_3
        private bool _disposed;
#endif

        public NativeMemoryCleaner(object self, Func<object, ICollection<TStack>> getContexts, SharedMultipleUseFlag lowMemoryFlag, TimeSpan period, TimeSpan idleTime)
        {
            _selfweakref = new WeakReference(self);

            _getContexts = () =>
            {
                object target = _selfweakref.Target;
                return target == null ? Array.Empty<TStack>() : getContexts(target);
            };
            _lowMemoryFlag = lowMemoryFlag;
            _idleTime = idleTime;
            _timer = new Timer(CleanNativeMemory, null, period, period);
        }

        public void CleanNativeMemory(object state)
        {
            var lockTaken = false;
            try
            {
                Monitor.TryEnter(_lock, ref lockTaken);
                if (lockTaken == false)
                    return;

#if NETSTANDARD1_3
                if (_disposed)
                    return;
#endif

                var now = DateTime.UtcNow;
                ICollection<TStack> values;
                try
                {
                    values = _getContexts();
                }
                catch (OutOfMemoryException)
                {
                    return; // trying to allocate the list? 
                }
                catch (ObjectDisposedException)
                {
                    return; // already disposed
                }
                foreach (var header in values)
                {
                    if (header == null)
                        continue;

                    var current = header.Head;
                    while (current != null)
                    {
                        var item = current.Value;
                        var parent = current;
                        current = current.Next;

                        if (item == null)
                            continue;

                        if (_lowMemoryFlag == false)
                        {
                            var timeInPool = now - item.InPoolSince;
                            if (timeInPool < _idleTime)
                                continue;
                        } // else dispose context on low mem stress

                        // it is too old, we can dispose it, but need to protect from races
                        // if the owner thread will just pick it up

                        if (!item.InUse.Raise())
                            continue;

                        try
                        {
                            item.Dispose();
                        }
                        catch (ObjectDisposedException)
                        {
                            // it is possible that this has already been disposed
                        }

                        parent.Value = null;
                    }
                }
            }
            finally
            {
                if (lockTaken)
                    Monitor.Exit(_lock);
            }
        }

        public void Dispose()
        {
#if !NETSTANDARD1_3
            using (var waitHandle = new ManualResetEvent(false))
            {
                if (_timer.Dispose(waitHandle))
                {
                    waitHandle.WaitOne();
                }
            }
#else
            lock (_lock) // prevent from running the callback _after_ dispose
            {
                _disposed = true;
                _timer.Dispose();
            }
#endif
        }
    }
}
