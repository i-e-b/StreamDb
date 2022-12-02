using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;

namespace StreamDb.Internal.Search
{
    /// <summary>
    /// Dictionary that generates keys when requested
    /// </summary>
    public class Map<TIdx, TVal> where TIdx : struct
    {
        [NotNull] private readonly Dictionary<TIdx, TVal> _data;
        private readonly Func<TVal>? _generator;

        /// <summary>
        /// Get value by index
        /// </summary>
        public TVal this[TIdx index]
        {
            get
            {
                if (_data.ContainsKey(index)) return _data[index];
                if (_generator != null) {
                    _data.Add(index, _generator());
                    return _data[index];
                }
                return default!;
            }
            set
            {
                if (_data.ContainsKey(index)) _data[index] = value;
                else _data.Add(index, value);
            }
        }

        /// <summary>
        /// Get known keys
        /// </summary>
        [NotNull]public IEnumerable<TIdx> Keys() => _data.Keys ?? throw new Exception("Map keys entry was invalid");
        /// <summary>
        /// True if the index is stored
        /// </summary>
        public bool Contains(TIdx idx) => _data.ContainsKey(idx);
        /// <summary>
        /// True if no keys are stored
        /// </summary>
        public bool IsEmpty() => _data.Count < 1;
        
        /// <summary>
        /// Remove all keys and values
        /// </summary>
        public void Clear() => _data.Clear();
        
        /// <summary>
        /// Return all values
        /// </summary>
        /// <returns></returns>
        [NotNull]public IEnumerable<KeyValuePair<TIdx, TVal>> All() => _data.Select(a=>a);

        /// <summary>
        /// Create a new map without a value generator
        /// </summary>
        public Map() { _data = new Dictionary<TIdx, TVal>(); }
        /// <summary>
        /// Create a new map with a generator for default values
        /// </summary>
        /// <param name="generator"></param>
        public Map(Func<TVal> generator) {
            _data = new Dictionary<TIdx, TVal>();
            _generator=generator;
        }

    }
}