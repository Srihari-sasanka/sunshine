console.log("Sunshine Services EV Platform loaded");

function showMessage(msg, type = "success") {
  const el = document.createElement("div");
  el.className = "alert alert-" + type;
  el.innerText = msg;
  document.body.prepend(el);
  setTimeout(() => el.remove(), 3000);
}

async function apiJson(url, options = {}) {
  const res = await fetch(url, options);
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || "Request failed");
  return data;
}
