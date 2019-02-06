(ns pppmap.core)



(defn print-progress [label index size]
  (println (Thread/currentThread) (java.util.Date.) "--" label " : " index " / " size))

(defn pmap-with-progress
  "Like map, except f is applied in parallel. Semi-lazy in that the
  parallel computation stays ahead of the consumption, but doesn't
  realize the entire result unless required. Only useful for
  computationally intensive functions where the time of f dominates
  the coordination overhead."
  {:added "1.0"
   :static true}
  ([label f coll]
   (let [n (+ 2 (.. Runtime getRuntime availableProcessors))
         size (count coll)
         f-with-print (fn [index item] (let [res (f item)
                                             _ (print-progress label index size)
                                             ]
                                         res
                                  ) )
         rets (map-indexed (fn [index item](future (f-with-print index item)  )) coll)
         step (fn step [[x & xs :as vs] fs]
                (lazy-seq
                 (if-let [s (seq fs)]
                   (cons (deref x) (step xs (rest s)))
                   (map deref vs))))]
     (step rets (drop n rets))))
  ([label f coll & colls]
   (let [step (fn step [cs]
                (lazy-seq
                 (let [ss (map seq cs)]
                   (when (every? identity ss)
                     (cons (map first ss) (step (map rest ss)))))))]
     (pmap-with-progress label #(apply f %) (step (cons coll colls))))))


(defn ppmap-with-progress
  "Partitioned pmap, for grouping map ops together to make parallel
  overhead worthwhile"
  [print-label grain-size f & colls]
  (apply concat
         (apply pmap-with-progress
                print-label
                (fn [& pgroups] (doall (apply map f pgroups)))
                (map (partial partition-all grain-size) colls))))


(defn ppmap
  "Partitioned pmap, for grouping map ops together to make parallel
  overhead worthwhile"
  [grain-size f & colls]
  (apply concat
   (apply pmap
          (fn [& pgroups] (doall (apply map f pgroups)))
          (map (partial partition-all grain-size) colls))))


(defn map-with-progress [print-label f col]
  "Like map, but with progress printing"
  (let [size (- (count col) 1)]
    (map-indexed (fn [index item]
                   (do (print-progress print-label index size)
                       (f item)
                       )
                   
                   ) col )))

