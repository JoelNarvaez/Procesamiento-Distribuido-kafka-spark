//const { faker } = require("@faker-js/faker");
const { faker } = require("@faker-js/faker/locale/es_MX");
const crypto = require("crypto");

//faker.locale = "es_MX";

const SERVIDORES = [
  {
    nombre: "server-01",
    ip: "192.168.1.65",
    zona: "rack-a",
    sistema_operativo: "Ubuntu Server 24.04",
    ambiente: "produccion"
  },
  {
    nombre: "server-02",
    ip: "192.168.1.66",
    zona: "rack-a",
    sistema_operativo: "Ubuntu Server 22.04",
    ambiente: "produccion"
  },
  {
    nombre: "server-03",
    ip: "192.168.1.67",
    zona: "rack-b",
    sistema_operativo: "Debian 12",
    ambiente: "produccion"
  },
  {
    nombre: "server-04",
    ip: "192.168.1.68",
    zona: "rack-b",
    sistema_operativo: "Rocky Linux 9",
    ambiente: "pruebas"
  },
  {
    nombre: "server-05",
    ip: "192.168.1.69",
    zona: "rack-c",
    sistema_operativo: "Ubuntu Server 24.04",
    ambiente: "desarrollo"
  }
];

const SERVICIOS = {
  resource: [
    "system-monitor",
    "node-exporter",
    "resource-agent",
    "hardware-monitor"
  ],
  request: [
    "api-gateway",
    "web-server",
    "payment-service",
    "catalog-service",
    "notification-service"
  ],
  error: [
    "database-service",
    "auth-service",
    "payment-service",
    "queue-service",
    "storage-service"
  ],
  network: [
    "network-agent",
    "load-balancer",
    "proxy-service",
    "firewall-agent"
  ],
  security: [
    "auth-service",
    "access-control",
    "security-agent",
    "identity-service"
  ]
};

const ENDPOINTS = [
  "/api/login",
  "/api/logout",
  "/api/usuarios",
  "/api/productos",
  "/api/pagos",
  "/api/reportes",
  "/api/notificaciones",
  "/api/ordenes",
  "/api/metricas",
  "/api/seguridad",
  "/api/carrito",
  "/api/inventario",
  "/api/facturas",
  "/api/clientes"
];

const METODOS_HTTP = ["GET", "POST", "PUT", "PATCH", "DELETE"];

const CIUDADES = [
  "Aguascalientes",
  "Guadalajara",
  "Ciudad de México",
  "Monterrey",
  "Zacatecas",
  "Querétaro",
  "León",
  "San Luis Potosí",
  "Puebla",
  "Toluca"
];

const MENSAJES = {
  resource: {
    INFO: [
      "Métricas del servidor registradas correctamente",
      "Uso de recursos dentro de parámetros normales",
      "Lectura de CPU, memoria y disco completada"
    ],
    WARNING: [
      "Uso elevado de recursos detectado",
      "Consumo de memoria superior al promedio",
      "Uso de disco cercano al umbral configurado"
    ],
    ERROR: [
      "No se pudo obtener una métrica del sistema",
      "Lectura de recursos incompleta",
      "Error al consultar información del sistema"
    ],
    CRITICAL: [
      "Servidor en estado crítico por consumo de recursos",
      "Uso extremo de CPU y memoria detectado",
      "Riesgo de saturación del servidor"
    ]
  },
  request: {
    INFO: [
      "Solicitud HTTP procesada correctamente",
      "Respuesta entregada dentro del tiempo esperado",
      "Petición atendida sin errores"
    ],
    WARNING: [
      "Tiempo de respuesta superior al promedio",
      "Incremento inusual de peticiones HTTP",
      "Endpoint con latencia moderada"
    ],
    ERROR: [
      "Error al procesar la solicitud HTTP",
      "El servicio respondió con error",
      "Fallo interno durante la petición"
    ],
    CRITICAL: [
      "Servicio HTTP no disponible",
      "Falla crítica en procesamiento de solicitudes",
      "Endpoint principal sin respuesta"
    ]
  },
  error: {
    INFO: [
      "Evento de diagnóstico registrado",
      "Revisión de estado completada",
      "Servicio revisado sin errores críticos"
    ],
    WARNING: [
      "Excepción controlada detectada",
      "Servicio con comportamiento inestable",
      "Advertencia registrada en el módulo"
    ],
    ERROR: [
      "Error interno del servicio",
      "Fallo de conexión con dependencia externa",
      "Excepción no controlada detectada"
    ],
    CRITICAL: [
      "Servicio detenido por error crítico",
      "Falla crítica detectada en el nodo",
      "Componente principal no disponible"
    ]
  },
  network: {
    INFO: [
      "Tráfico de red dentro de parámetros normales",
      "Conectividad validada correctamente",
      "Paquetes transmitidos sin pérdida significativa"
    ],
    WARNING: [
      "Latencia superior al promedio",
      "Incremento de conexiones activas detectado",
      "Tráfico de red elevado"
    ],
    ERROR: [
      "Pérdida de paquetes detectada",
      "Error de conexión entre servicios",
      "Tiempo de espera agotado en comunicación interna"
    ],
    CRITICAL: [
      "Interrupción crítica de red",
      "Nodo sin comunicación estable",
      "Falla crítica de conectividad"
    ]
  },
  security: {
    INFO: [
      "Inicio de sesión exitoso",
      "Acceso autorizado correctamente",
      "Token de sesión validado"
    ],
    WARNING: [
      "Intentos de acceso fallidos consecutivos",
      "Actividad inusual detectada en la cuenta",
      "Acceso desde ubicación poco frecuente"
    ],
    ERROR: [
      "Autenticación fallida",
      "Token inválido o expirado",
      "Intento de acceso rechazado"
    ],
    CRITICAL: [
      "Posible ataque de fuerza bruta detectado",
      "Acceso crítico bloqueado por reglas de seguridad",
      "IP sospechosa bloqueada temporalmente"
    ]
  }
};

function randomItem(lista) {
  return lista[Math.floor(Math.random() * lista.length)];
}

function randomInt(min, max) {
  return faker.number.int({ min, max });
}

function randomDecimal(min, max) {
  return Number(faker.number.float({ min, max, fractionDigits: 2 }).toFixed(2));
}

function aplicarVariacion(valor, cap = Infinity) {
  if (Math.random() < 0.18) {
    valor *= Math.random() < 0.5
      ? randomDecimal(0.25, 0.7)   // caída
      : randomDecimal(1.4, 2.4);   // pico
  }
  valor *= 1 + (Math.random() - 0.5) * 0.3;
  return Math.max(0, Math.min(cap, valor));
}

function varInt(min, max, cap = Infinity) {
  return Math.round(aplicarVariacion(randomInt(min, max), cap));
}

function varDecimal(min, max, cap = Infinity) {
  return Number(aplicarVariacion(randomDecimal(min, max), cap).toFixed(2));
}

function generarId() {
  return crypto.randomUUID();
}

function generarNivel(tipoEvento) {
  if (tipoEvento === "error") {
    return randomItem(["ERROR", "ERROR", "ERROR", "WARNING", "CRITICAL"]);
  }

  if (tipoEvento === "security") {
    return randomItem(["INFO", "INFO", "WARNING", "WARNING", "ERROR", "CRITICAL"]);
  }

  if (tipoEvento === "network") {
    return randomItem(["INFO", "INFO", "INFO", "WARNING", "ERROR"]);
  }

  if (tipoEvento === "resource") {
    return randomItem(["INFO", "INFO", "INFO", "WARNING", "WARNING", "ERROR", "CRITICAL"]);
  }

  return randomItem(["INFO", "INFO", "INFO", "INFO", "WARNING", "ERROR"]);
}

function generarCodigoEstado(nivel, tipoEvento) {
  if (tipoEvento !== "request") {
    if (nivel === "INFO") return 200;
    if (nivel === "WARNING") return randomItem([300, 400, 401, 403, 404]);
    return randomItem([500, 502, 503, 504]);
  }

  if (nivel === "INFO") return randomItem([200, 201, 202, 204]);
  if (nivel === "WARNING") return randomItem([301, 302, 400, 401, 403, 404, 408, 429]);
  if (nivel === "ERROR") return randomItem([500, 502, 503, 504]);
  return randomItem([500, 503, 504]);
}

function generarMetricasPorNivel(nivel) {
  if (nivel === "CRITICAL") {
    return {
      tiempo_respuesta_ms: varInt(1500, 6000),
      uso_cpu_porcentaje: varDecimal(82, 99.5, 100),
      uso_ram_porcentaje: varDecimal(80, 99.5, 100),
      uso_disco_porcentaje: varDecimal(78, 99, 100),
      bytes_entrada: varInt(250000, 1500000),
      bytes_salida: varInt(400000, 2500000),
      peticiones_por_minuto: varInt(2500, 7000),
      conexiones_activas: varInt(900, 3000),
      errores_minuto: varInt(25, 120),
      latencia_red_ms: varInt(300, 1500),
      temperatura_cpu: varDecimal(82, 105, 115),
      paquetes_perdidos: varInt(50, 500)
    };
  }

  if (nivel === "ERROR") {
    return {
      tiempo_respuesta_ms: varInt(700, 3000),
      uso_cpu_porcentaje: varDecimal(55, 92, 100),
      uso_ram_porcentaje: varDecimal(52, 94, 100),
      uso_disco_porcentaje: varDecimal(45, 90, 100),
      bytes_entrada: varInt(100000, 900000),
      bytes_salida: varInt(150000, 1500000),
      peticiones_por_minuto: varInt(1200, 4000),
      conexiones_activas: varInt(400, 1600),
      errores_minuto: varInt(8, 50),
      latencia_red_ms: varInt(120, 700),
      temperatura_cpu: varDecimal(62, 92, 115),
      paquetes_perdidos: varInt(10, 150)
    };
  }

  if (nivel === "WARNING") {
    return {
      tiempo_respuesta_ms: varInt(300, 1200),
      uso_cpu_porcentaje: varDecimal(40, 82, 100),
      uso_ram_porcentaje: varDecimal(38, 86, 100),
      uso_disco_porcentaje: varDecimal(35, 88, 100),
      bytes_entrada: varInt(50000, 500000),
      bytes_salida: varInt(70000, 900000),
      peticiones_por_minuto: varInt(600, 2500),
      conexiones_activas: varInt(200, 1000),
      errores_minuto: varInt(1, 12),
      latencia_red_ms: varInt(70, 300),
      temperatura_cpu: varDecimal(52, 80, 115),
      paquetes_perdidos: varInt(1, 40)
    };
  }

  return {
    tiempo_respuesta_ms: varInt(20, 350),
    uso_cpu_porcentaje: varDecimal(3, 55, 100),
    uso_ram_porcentaje: varDecimal(8, 65, 100),
    uso_disco_porcentaje: varDecimal(12, 70, 100),
    bytes_entrada: varInt(500, 150000),
    bytes_salida: varInt(500, 250000),
    peticiones_por_minuto: varInt(10, 900),
    conexiones_activas: varInt(1, 350),
    errores_minuto: varInt(0, 3),
    latencia_red_ms: varInt(1, 90),
    temperatura_cpu: varDecimal(32, 65, 115),
    paquetes_perdidos: varInt(0, 5)
  };
}

function generarDatosHttp(tipoEvento, nivel) {
  if (tipoEvento !== "request") {
    return {
      metodo_http: null,
      user_agent: null,
      ip_origen: faker.internet.ipv4(),
      endpoint: randomItem(ENDPOINTS)
    };
  }

  return {
    metodo_http: randomItem(METODOS_HTTP),
    user_agent: faker.internet.userAgent(),
    ip_origen: faker.internet.ipv4(),
    endpoint: randomItem(ENDPOINTS)
  };
}

function generarDatosSeguridad(tipoEvento, nivel) {
  const esSeguridad = tipoEvento === "security";

  return {
    intento_login: esSeguridad ? randomItem(["exitoso", "fallido", "bloqueado"]) : null,
    ip_sospechosa: esSeguridad && ["WARNING", "ERROR", "CRITICAL"].includes(nivel),
    pais_origen: esSeguridad ? faker.location.country() : null
  };
}

function generarEvento(tipoEvento) {
  const servidor = randomItem(SERVIDORES);
  const nivel = generarNivel(tipoEvento);
  const metricas = generarMetricasPorNivel(nivel);
  const http = generarDatosHttp(tipoEvento, nivel);
  const seguridad = generarDatosSeguridad(tipoEvento, nivel);
  const servicio = randomItem(SERVICIOS[tipoEvento] || SERVICIOS.request);

  return {
    id_log: generarId(),
    timestamp_evento: new Date().toISOString(),

    servidor: servidor.nombre,
    ip_servidor: servidor.ip,
    zona: servidor.zona,
    sistema_operativo: servidor.sistema_operativo,
    ambiente: servidor.ambiente,

    servicio,
    tipo_evento: tipoEvento,
    nivel,
    codigo_estado: generarCodigoEstado(nivel, tipoEvento),

    endpoint: http.endpoint,
    metodo_http: http.metodo_http,
    usuario: faker.internet.username(),
    correo_usuario: faker.internet.email(),
    ciudad: randomItem(CIUDADES),
    ip_origen: http.ip_origen,
    user_agent: http.user_agent,

    tiempo_respuesta_ms: metricas.tiempo_respuesta_ms,
    uso_cpu_porcentaje: metricas.uso_cpu_porcentaje,
    uso_ram_porcentaje: metricas.uso_ram_porcentaje,
    uso_disco_porcentaje: metricas.uso_disco_porcentaje,
    bytes_entrada: metricas.bytes_entrada,
    bytes_salida: metricas.bytes_salida,
    peticiones_por_minuto: metricas.peticiones_por_minuto,
    conexiones_activas: metricas.conexiones_activas,
    errores_minuto: metricas.errores_minuto,
    latencia_red_ms: metricas.latencia_red_ms,
    temperatura_cpu: metricas.temperatura_cpu,
    paquetes_perdidos: metricas.paquetes_perdidos,

    intento_login: seguridad.intento_login,
    ip_sospechosa: seguridad.ip_sospechosa,
    pais_origen: seguridad.pais_origen,

    trace_id: faker.string.uuid(),
    session_id: faker.string.alphanumeric(24),
    proceso: `${servicio}-${randomInt(1000, 9999)}`,
    mensaje: randomItem(MENSAJES[tipoEvento][nivel])
  };
}

module.exports = {
  generarEvento
};