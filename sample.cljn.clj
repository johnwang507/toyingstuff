defn 
  svc-http
  [opts]
  let {:keys [indexnum seperator port]} opts 
      idx (atom 0N)
    run-jetty 
      fn request
        if indexnum
           response str 
                    swap! idx inc
                    seperator (gen-row opts)
           response (gen-row opts)
      :port port

(defn svc-http [opts]
  (let [{:keys [indexnum seperator port]} opts idx (atom 0N)]
    (run-jetty 
      (fn [request] 
        (if indexnum
          (response (str (swap! idx inc) seperator (gen-row opts)))
          (response (gen-row opts))))
      {:port port})))
