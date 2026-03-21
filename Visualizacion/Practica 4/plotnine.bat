 curl http://gpu1.esit.ull.es:4000/v1/chat/completions  ^
  -H "Content-Type: application/json" ^
  -H "Authorization: Bearer sk-1234" ^
  -d "{ \"model\": \"ollama/deepseek-coder:6.7b-instruct-q4_K_M\", \"messages\": [{\"role\": \"system\", \"content\": \"Eres un experto en Python. Vas a crear solo codigo python. Además eres un experto en Gestalt.  No des explicaciones.\"}, {\"role\": \"user\", \"content\": \"Tengo un DataFrame con columnas [ano, isla, medida, valor]. Islas: [Tenerife, Gran Canaria]. Genera codigo Python con plotnine: la funcion debe llamarse generar_plot(df), destacar Tenerife en azul (#007bff) y el resto en gris (#D3D3D3) para aplicar Punto Focal.\"}],\"temperature\": 0.7, \"max_tokens\": 500,\"top_p\": 0.9}"
