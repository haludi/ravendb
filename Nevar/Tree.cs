﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;

namespace Nevar
{
	public unsafe class Tree
	{
		public int BranchPages;
		public int LeafPages;
		public int OverflowPages;
		public int Depth;
		public int PageCount;

		private readonly SliceComparer _cmp;

		public Page Root;

		private Tree(SliceComparer cmp, Page root)
		{
			_cmp = cmp;
			Root = root;
		}

		public static Tree CreateOrOpen(Transaction tx, int root, SliceComparer cmp)
		{
			if (root != -1)
			{
				return new Tree(cmp, tx.GetPage(root));
			}

			// need to create the root
			var newRootPage = NewPage(tx, PageFlags.Leaf, 1);
			var tree = new Tree(cmp, newRootPage) { Depth = 1 };
			var cursor = tx.GetCursor(tree);
			cursor.RecordNewPage(newRootPage, 1);
			return tree;
		}

		public void Add(Transaction tx, Slice key, Stream value)
		{
			if (value == null) throw new ArgumentNullException("value");
			if (value.Length > int.MaxValue) throw new ArgumentException("Cannot add a value that is over 2GB in size", "value");

			var cursor = tx.GetCursor(this);

			var page = FindPageFor(tx, key, cursor);

			if (page.LastMatch == 0) // this is an update operation
				page.RemoveNode(page.LastSearchPosition);

			if (page.HasSpaceFor(key, value) == false)
			{
				new PageSplitter(tx, this, key, value, -1, cursor).Execute();
				DebugValidateTree(tx, cursor.Root);
				return;
			}

			page.AddNode(page.LastSearchPosition, key, value, 0);

			page.DebugValidate(_cmp);
		}

		[Conditional("DEBUG")]
		private void DebugValidateTree(Transaction tx, Page root)
		{
			var stack = new Stack<Page>();
			stack.Push(root);
			while (stack.Count > 0)
			{
				var p = stack.Pop();
				p.DebugValidate(_cmp);
				if (p.IsBranch == false)
					continue;
				for (int i = 0; i < p.NumberOfEntries; i++)
				{
					stack.Push(tx.GetPage(p.GetNode(i)->PageNumber));
				}
			}
		}


		/// <summary>
		/// For leaf pages, check the split point based on what
		///	fits where, since otherwise adding the node can fail.
		///	
		///	This check is only needed when the data items are
		///	relatively large, such that being off by one will
		///	make the difference between success or failure.
		///	
		///	It's also relevant if a page happens to be laid out
		///	such that one half of its nodes are all "small" and
		///	the other half of its nodes are "large." If the new
		///	item is also "large" and falls on the half with
		///	"large" nodes, it also may not fit.
		/// </summary>
		private static ushort AdjustSplitPosition(Slice key, Stream value, Page page, ushort currentIndex, ushort splitIndex,
													  ref bool newPosition)
		{
			var nodeSize = SizeOf.NodeEntry(key, value) + Constants.NodeOffsetSize;
			if (page.NumberOfEntries >= 20 && nodeSize <= Constants.PageMaxSpace / 16)
			{
				return splitIndex;
			}

			int pageSize = nodeSize;
			if (currentIndex <= splitIndex)
			{
				newPosition = false;
				for (int i = 0; i < splitIndex; i++)
				{
					var node = page.GetNode(i);
					pageSize += node->GetNodeSize();
					pageSize += pageSize & 1;
					if (pageSize > Constants.PageMaxSpace)
					{
						if (i <= currentIndex)
						{
							if (i < currentIndex)
								newPosition = true;
							return currentIndex;
						}
						return (ushort)i;
					}
				}
			}
			else
			{
				for (int i = page.NumberOfEntries - 1; i >= splitIndex; i--)
				{
					var node = page.GetNode(i);
					pageSize += node->GetNodeSize();
					pageSize += pageSize & 1;
					if (pageSize > Constants.PageMaxSpace)
					{
						if (i >= currentIndex)
						{
							newPosition = false;
							return currentIndex;
						}
						return (ushort)(i + 1);
					}
				}
			}
			return splitIndex;
		}

		public Page FindPageFor(Transaction tx, Slice key, Cursor cursor)
		{
			var p = cursor.Root;
			cursor.Push(p);
			while (p.Flags.HasFlag(PageFlags.Branch))
			{
				ushort nodePos;
				if (key.Options == SliceOptions.BeforeAllKeys)
				{
					nodePos = 0;
				}
				else if (key.Options == SliceOptions.AfterAllKeys)
				{
					nodePos = (ushort)(p.NumberOfEntries - 1);
				}
				else
				{
					if (p.Search(key, _cmp) != null)
					{
						nodePos = p.LastSearchPosition;
						if (p.LastMatch != 0)
							nodePos--;
					}
					else
					{
						nodePos = (ushort)(p.LastSearchPosition - 1);
					}

				}

				var node = p.GetNode(nodePos);
				p = tx.GetPage(node->PageNumber);
				cursor.Push(p);
			}

			if (p.IsLeaf == false)
				throw new DataException("Index points to a non leaf page");

			p.NodePositionFor(key, _cmp); // will set the LastSearchPosition

			return p;
		}

		private static Page NewPage(Transaction tx, PageFlags flags, int num)
		{
			var page = tx.AllocatePage(num);

			page.Flags = flags;

			return page;
		}

		public void Delete(Transaction tx, Slice key)
		{
			var cursor = tx.GetCursor(this);

			var page = FindPageFor(tx, key, cursor);

			var pos = page.NodePositionFor(key, _cmp);
			if (page.LastMatch != 0)
				return; // not an exact match, can't delete
			page.RemoveNode(pos);

			page.DebugValidate(_cmp);

		}

		public class PageSplitter
		{
			private readonly Transaction _tx;
			private readonly Tree _parent;
			private readonly Slice _newKey;
			private readonly Stream _value;
			private readonly int _pageNumber;
			private readonly Cursor _cursor;

			public PageSplitter(Transaction tx, Tree parent, Slice newKey, Stream value, int pageNumber, Cursor cursor)
			{
				_tx = tx;
				_parent = parent;
				_newKey = newKey;
				_value = value;
				_pageNumber = pageNumber;
				_cursor = cursor;
			}

			public void Execute()
			{
				Page parentPage;
				var page = _cursor.Pop();
				var newPosition = true;
				var currentIndex = page.LastSearchPosition;
				var rightPage = NewPage(_tx, page.Flags, 1);
				_cursor.RecordNewPage(page, 1);
				rightPage.Flags = page.Flags;
				if (_cursor.Pages.Count == 0) // we need to do a root split
				{
					var newRootPage = NewPage(_tx, PageFlags.Branch, 1);
					_cursor.Push(newRootPage);
					_cursor.Root = newRootPage;
					_cursor.Depth++;
					_cursor.RecordNewPage(newRootPage, 1);

					// now add implicit left page
					newRootPage.AddNode(0, new Slice(SliceOptions.BeforeAllKeys), null, page.PageNumber);
					parentPage = newRootPage;
					parentPage.LastSearchPosition++;
				}
				else
				{
					// we already popped the page, so the current one on the stack is what the parent of the page
					parentPage = _cursor.CurrentPage;
				}

				var splitIndex = (ushort)(page.NumberOfEntries / 2);
				if (currentIndex < splitIndex)
					newPosition = false;

				if (page.IsLeaf)
				{
					splitIndex = AdjustSplitPosition(_newKey, _value, page, currentIndex, splitIndex, ref newPosition);
				}

				// here we the current key is the separator key and can go either way, so 
				// use newPosition to decide if it stays on the left node or moves to the right
				Slice seperatorKey;
				if (currentIndex == splitIndex && newPosition)
				{
					seperatorKey = _newKey;
				}
				else
				{
					var node = page.GetNode(splitIndex);
					seperatorKey = new Slice(node);
				}

				if (parentPage.SizeLeft < SizeOf.BranchEntry(seperatorKey) + Constants.NodeOffsetSize)
				{
					new PageSplitter(_tx, _parent, seperatorKey, null, rightPage.PageNumber, _cursor).Execute();
				}
				else
				{
					parentPage.AddNode(parentPage.LastSearchPosition, seperatorKey, null, rightPage.PageNumber);
				}
				// move the actual entries from page to right page
				var nKeys = page.NumberOfEntries;
				for (ushort i = splitIndex; i < nKeys; i++)
				{
					var node = page.GetNode(i);
					rightPage.CopyNodeData(node);
				}
				page.Truncate(_tx, splitIndex);

				// actually insert the new key
				if (currentIndex > splitIndex ||
					newPosition && currentIndex == splitIndex)
				{
					var pos = rightPage.NodePositionFor(_newKey, _parent._cmp);
					rightPage.AddNode(pos, _newKey, _value, _pageNumber);
					_cursor.Push(rightPage);
				}
				else
				{
					page.AddNode(page.LastSearchPosition, _newKey, _value, _pageNumber);
					_cursor.Push(page);
				}
			}
		}
	}

	
}