var FA=Object.defineProperty,LA=Object.defineProperties;var VA=Object.getOwnPropertyDescriptors;var td=Object.getOwnPropertySymbols;var tD=Object.prototype.hasOwnProperty,nD=Object.prototype.propertyIsEnumerable;var eD=(t,n,e)=>n in t?FA(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,y=(t,n)=>{for(var e in n||={})tD.call(n,e)&&eD(t,e,n[e]);if(td)for(var e of td(n))nD.call(n,e)&&eD(t,e,n[e]);return t},te=(t,n)=>LA(t,VA(n));var iD=(t,n)=>{var e={};for(var i in t)tD.call(t,i)&&n.indexOf(i)<0&&(e[i]=t[i]);if(t!=null&&td)for(var i of td(t))n.indexOf(i)<0&&nD.call(t,i)&&(e[i]=t[i]);return e};var At=null,nd=!1,Pm=1,jA=null,it=Symbol("SIGNAL");function X(t){let n=At;return At=t,n}function id(){return At}var Xi={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Ji(t){if(nd)throw new Error("");if(At===null)return;At.consumerOnSignalRead(t);let n=At.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=At.recomputing;if(i&&(e=n!==void 0?n.nextProducer:At.producers,e!==void 0&&e.producer===t)){At.producersTail=e,e.lastReadVersion=t.version;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===At&&(!i||UA(r,At)))return;let o=es(At),s={producer:t,consumer:At,nextProducer:e,prevConsumer:r,lastReadVersion:t.version,nextConsumer:void 0};At.producersTail=s,n!==void 0?n.nextProducer=s:At.producers=s,o&&aD(t,s)}function rD(){Pm++}function Yr(t){if(!(es(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===Pm)){if(!t.producerMustRecompute(t)&&!Jo(t)){Xo(t);return}t.producerRecomputeValue(t),Xo(t)}}function Fm(t){if(t.consumers===void 0)return;let n=nd;nd=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||BA(i)}}finally{nd=n}}function Lm(){return At?.consumerAllowSignalWrites!==!1}function BA(t){t.dirty=!0,Fm(t),t.consumerMarkedDirty?.(t)}function Xo(t){t.dirty=!1,t.lastCleanEpoch=Pm}function Ci(t){return t&&oD(t),X(t)}function oD(t){t.producersTail=void 0,t.recomputing=!0}function er(t,n){X(n),t&&sD(t)}function sD(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(es(t))do e=Vm(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function Jo(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(Yr(e),i!==e.version))return!0}return!1}function tr(t){if(es(t)){let n=t.producers;for(;n!==void 0;)n=Vm(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function aD(t,n){let e=t.consumersTail,i=es(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)aD(r.producer,r)}function Vm(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!es(n)){let o=n.producers;for(;o!==void 0;)o=Vm(o)}return e}function es(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function Na(t){jA?.(t)}function UA(t,n){let e=n.producersTail;if(e!==void 0){let i=n.producers;do{if(i===t)return!0;if(i===e)break;i=i.nextProducer}while(i!==void 0)}return!1}function Pa(t,n){return Object.is(t,n)}function Fa(t,n){let e=Object.create(HA);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(Yr(e),Ji(e),e.value===Yn)throw e.error;return e.value};return i[it]=e,Na(e),i}var Wr=Symbol("UNSET"),qr=Symbol("COMPUTING"),Yn=Symbol("ERRORED"),HA=te(y({},Xi),{value:Wr,dirty:!0,error:null,equal:Pa,kind:"computed",producerMustRecompute(t){return t.value===Wr||t.value===qr},producerRecomputeValue(t){if(t.value===qr)throw new Error("");let n=t.value;t.value=qr;let e=Ci(t),i,r=!1;try{i=t.computation(),X(null),r=n!==Wr&&n!==Yn&&i!==Yn&&t.equal(n,i)}catch(o){i=Yn,t.error=o}finally{er(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function zA(){throw new Error}var lD=zA;function cD(t){lD(t)}function jm(t){lD=t}var $A=null;function Bm(t,n){let e=Object.create(La);e.value=t,n!==void 0&&(e.equal=n);let i=()=>dD(e);return i[it]=e,Na(e),[i,s=>Zr(e,s),s=>rd(e,s)]}function dD(t){return Ji(t),t.value}function Zr(t,n){Lm()||cD(t),t.equal(t.value,n)||(t.value=n,GA(t))}function rd(t,n){Lm()||cD(t),Zr(t,n(t.value))}var La=te(y({},Xi),{equal:Pa,value:void 0,kind:"signal"});function GA(t){t.version++,rD(),Fm(t),$A?.(t)}var Um=te(y({},Xi),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Hm(t){if(t.dirty=!1,t.version>0&&!Jo(t))return;t.version++;let n=Ci(t);try{t.cleanup(),t.fn()}finally{er(t,n)}}function ge(t){return typeof t=="function"}function ts(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var od=ts(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function Kr(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var ae=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(ge(i))try{i()}catch(o){n=o instanceof od?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{uD(o)}catch(s){n=n??[],s instanceof od?n=[...n,...s.errors]:n.push(s)}}if(n)throw new od(n)}}add(n){var e;if(n&&n!==this)if(this.closed)uD(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&Kr(e,n)}remove(n){let{_finalizers:e}=this;e&&Kr(e,n),n instanceof t&&n._removeParent(this)}};ae.EMPTY=(()=>{let t=new ae;return t.closed=!0,t})();var zm=ae.EMPTY;function sd(t){return t instanceof ae||t&&"closed"in t&&ge(t.remove)&&ge(t.add)&&ge(t.unsubscribe)}function uD(t){ge(t)?t():t.unsubscribe()}var En={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var ns={setTimeout(t,n,...e){let{delegate:i}=ns;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=ns;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function ad(t){ns.setTimeout(()=>{let{onUnhandledError:n}=En;if(n)n(t);else throw t})}function Va(){}var fD=$m("C",void 0,void 0);function hD(t){return $m("E",void 0,t)}function mD(t){return $m("N",t,void 0)}function $m(t,n,e){return{kind:t,value:n,error:e}}var Qr=null;function is(t){if(En.useDeprecatedSynchronousErrorHandling){let n=!Qr;if(n&&(Qr={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=Qr;if(Qr=null,e)throw i}}else t()}function pD(t){En.useDeprecatedSynchronousErrorHandling&&Qr&&(Qr.errorThrown=!0,Qr.error=t)}var Xr=class extends ae{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,sd(n)&&n.add(this)):this.destination=YA}static create(n,e,i){return new Ei(n,e,i)}next(n){this.isStopped?Wm(mD(n),this):this._next(n)}error(n){this.isStopped?Wm(hD(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Wm(fD,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},WA=Function.prototype.bind;function Gm(t,n){return WA.call(t,n)}var qm=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){ld(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){ld(i)}else ld(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){ld(e)}}},Ei=class extends Xr{constructor(n,e,i){super();let r;if(ge(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&En.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&Gm(n.next,o),error:n.error&&Gm(n.error,o),complete:n.complete&&Gm(n.complete,o)}):r=n}this.destination=new qm(r)}};function ld(t){En.useDeprecatedSynchronousErrorHandling?pD(t):ad(t)}function qA(t){throw t}function Wm(t,n){let{onStoppedNotification:e}=En;e&&ns.setTimeout(()=>e(t,n))}var YA={closed:!0,next:Va,error:qA,complete:Va};var rs=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Qt(t){return t}function Ym(...t){return Zm(t)}function Zm(t){return t.length===0?Qt:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var de=(()=>{class t{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new t;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=KA(e)?e:new Ei(e,i,r);return is(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=gD(i),new i((r,o)=>{let s=new Ei({next:a=>{try{e(a)}catch(l){o(l),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[rs](){return this}pipe(...e){return Zm(e)(this)}toPromise(e){return e=gD(e),new e((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return t.create=n=>new t(n),t})();function gD(t){var n;return(n=t??En.Promise)!==null&&n!==void 0?n:Promise}function ZA(t){return t&&ge(t.next)&&ge(t.error)&&ge(t.complete)}function KA(t){return t&&t instanceof Xr||ZA(t)&&sd(t)}function Km(t){return ge(t?.lift)}function me(t){return n=>{if(Km(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function ue(t,n,e,i,r){return new Qm(t,n,e,i,r)}var Qm=class extends Xr{constructor(n,e,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(l){n.error(l)}}:super._next,this._error=r?function(a){try{r(a)}catch(l){n.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};function vD(){return me((t,n)=>{let e=null;t._refCount++;let i=ue(n,void 0,void 0,void 0,()=>{if(!t||t._refCount<=0||0<--t._refCount){e=null;return}let r=t._connection,o=e;e=null,r&&(!o||r===o)&&r.unsubscribe(),n.unsubscribe()});t.subscribe(i),i.closed||(e=t.connect())})}var ja=class extends de{constructor(n,e){super(),this.source=n,this.subjectFactory=e,this._subject=null,this._refCount=0,this._connection=null,Km(n)&&(this.lift=n.lift)}_subscribe(n){return this.getSubject().subscribe(n)}getSubject(){let n=this._subject;return(!n||n.isStopped)&&(this._subject=this.subjectFactory()),this._subject}_teardown(){this._refCount=0;let{_connection:n}=this;this._subject=this._connection=null,n?.unsubscribe()}connect(){let n=this._connection;if(!n){n=this._connection=new ae;let e=this.getSubject();n.add(this.source.subscribe(ue(e,void 0,()=>{this._teardown(),e.complete()},i=>{this._teardown(),e.error(i)},()=>this._teardown()))),n.closed&&(this._connection=null,n=ae.EMPTY)}return n}refCount(){return vD()(this)}};var os={schedule(t){let n=requestAnimationFrame,e=cancelAnimationFrame,{delegate:i}=os;i&&(n=i.requestAnimationFrame,e=i.cancelAnimationFrame);let r=n(o=>{e=void 0,t(o)});return new ae(()=>e?.(r))},requestAnimationFrame(...t){let{delegate:n}=os;return(n?.requestAnimationFrame||requestAnimationFrame)(...t)},cancelAnimationFrame(...t){let{delegate:n}=os;return(n?.cancelAnimationFrame||cancelAnimationFrame)(...t)},delegate:void 0};var _D=ts(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var x=(()=>{class t extends de{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new cd(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new _D}next(e){is(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){is(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){is(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?zm:(this.currentObservers=null,o.push(e),new ae(()=>{this.currentObservers=null,Kr(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new de;return e.source=this,e}}return t.create=(n,e)=>new cd(n,e),t})(),cd=class extends x{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:zm}};var rt=class extends x{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var Ba={now(){return(Ba.delegate||Date).now()},delegate:void 0};var nr=class extends x{constructor(n=1/0,e=1/0,i=Ba){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;e||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=e.now(),a=0;for(let l=1;l<i.length&&i[l]<=s;l+=2)a=l;a&&i.splice(0,a+1)}}};var dd=class extends ae{constructor(n,e){super()}schedule(n,e=0){return this}};var Ua={setInterval(t,n,...e){let{delegate:i}=Ua;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=Ua;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var ir=class extends dd{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return Ua.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&Ua.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,Kr(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var QA=1,Xm,Jm={};function yD(t){return t in Jm?(delete Jm[t],!0):!1}var bD={setImmediate(t){let n=QA++;return Jm[n]=!0,Xm||(Xm=Promise.resolve()),Xm.then(()=>yD(n)&&t()),n},clearImmediate(t){yD(t)}};var{setImmediate:XA,clearImmediate:JA}=bD,Ha={setImmediate(...t){let{delegate:n}=Ha;return(n?.setImmediate||XA)(...t)},clearImmediate(t){let{delegate:n}=Ha;return(n?.clearImmediate||JA)(t)},delegate:void 0};var ud=class extends ir{constructor(n,e){super(n,e),this.scheduler=n,this.work=e}requestAsyncId(n,e,i=0){return i!==null&&i>0?super.requestAsyncId(n,e,i):(n.actions.push(this),n._scheduled||(n._scheduled=Ha.setImmediate(n.flush.bind(n,void 0))))}recycleAsyncId(n,e,i=0){var r;if(i!=null?i>0:this.delay>0)return super.recycleAsyncId(n,e,i);let{actions:o}=n;e!=null&&((r=o[o.length-1])===null||r===void 0?void 0:r.id)!==e&&(Ha.clearImmediate(e),n._scheduled===e&&(n._scheduled=void 0))}};var ss=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};ss.now=Ba.now;var rr=class extends ss{constructor(n,e=ss.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var fd=class extends rr{flush(n){this._active=!0;let e=this._scheduled;this._scheduled=void 0;let{actions:i}=this,r;n=n||i.shift();do if(r=n.execute(n.state,n.delay))break;while((n=i[0])&&n.id===e&&i.shift());if(this._active=!1,r){for(;(n=i[0])&&n.id===e&&i.shift();)n.unsubscribe();throw r}}};var hd=new fd(ud);var za=new rr(ir),DD=za;var md=class extends ir{constructor(n,e){super(n,e),this.scheduler=n,this.work=e}requestAsyncId(n,e,i=0){return i!==null&&i>0?super.requestAsyncId(n,e,i):(n.actions.push(this),n._scheduled||(n._scheduled=os.requestAnimationFrame(()=>n.flush(void 0))))}recycleAsyncId(n,e,i=0){var r;if(i!=null?i>0:this.delay>0)return super.recycleAsyncId(n,e,i);let{actions:o}=n;e!=null&&e===n._scheduled&&((r=o[o.length-1])===null||r===void 0?void 0:r.id)!==e&&(os.cancelAnimationFrame(e),n._scheduled=void 0)}};var pd=class extends rr{flush(n){this._active=!0;let e;n?e=n.id:(e=this._scheduled,this._scheduled=void 0);let{actions:i}=this,r;n=n||i.shift();do if(r=n.execute(n.state,n.delay))break;while((n=i[0])&&n.id===e&&i.shift());if(this._active=!1,r){for(;(n=i[0])&&n.id===e&&i.shift();)n.unsubscribe();throw r}}};var gd=new pd(md);var We=new de(t=>t.complete());function vd(t){return t&&ge(t.schedule)}function ep(t){return t[t.length-1]}function _d(t){return ge(ep(t))?t.pop():void 0}function Zn(t){return vd(ep(t))?t.pop():void 0}function wD(t,n){return typeof ep(t)=="number"?t.pop():n}function ED(t,n,e,i){function r(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(u){try{c(i.next(u))}catch(f){s(f)}}function l(u){try{c(i.throw(u))}catch(f){s(f)}}function c(u){u.done?o(u.value):r(u.value).then(a,l)}c((i=i.apply(t,n||[])).next())})}function CD(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function Jr(t){return this instanceof Jr?(this.v=t,this):new Jr(t)}function xD(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(m){return function(p){return Promise.resolve(p).then(m,f)}}function a(m,p){i[m]&&(r[m]=function(w){return new Promise(function(E,I){o.push([m,w,E,I])>1||l(m,w)})},p&&(r[m]=p(r[m])))}function l(m,p){try{c(i[m](p))}catch(w){h(o[0][3],w)}}function c(m){m.value instanceof Jr?Promise.resolve(m.value.v).then(u,f):h(o[0][2],m)}function u(m){l("next",m)}function f(m){l("throw",m)}function h(m,p){m(p),o.shift(),o.length&&l(o[0][0],o[0][1])}}function SD(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof CD=="function"?CD(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(s){return new Promise(function(a,l){s=t[o](s),r(a,l,s.done,s.value)})}}function r(o,s,a,l){Promise.resolve(l).then(function(c){o({value:c,done:a})},s)}}var yd=t=>t&&typeof t.length=="number"&&typeof t!="function";function bd(t){return ge(t?.then)}function Dd(t){return ge(t[rs])}function wd(t){return Symbol.asyncIterator&&ge(t?.[Symbol.asyncIterator])}function Cd(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function eR(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Ed=eR();function xd(t){return ge(t?.[Ed])}function Sd(t){return xD(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield Jr(e.read());if(r)return yield Jr(void 0);yield yield Jr(i)}}finally{e.releaseLock()}})}function Id(t){return ge(t?.getReader)}function Ve(t){if(t instanceof de)return t;if(t!=null){if(Dd(t))return tR(t);if(yd(t))return nR(t);if(bd(t))return iR(t);if(wd(t))return ID(t);if(xd(t))return rR(t);if(Id(t))return oR(t)}throw Cd(t)}function tR(t){return new de(n=>{let e=t[rs]();if(ge(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function nR(t){return new de(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function iR(t){return new de(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,ad)})}function rR(t){return new de(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function ID(t){return new de(n=>{sR(t,n).catch(e=>n.error(e))})}function oR(t){return ID(Sd(t))}function sR(t,n){var e,i,r,o;return ED(this,void 0,void 0,function*(){try{for(e=SD(t);i=yield e.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function qt(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function Md(t,n=0){return me((e,i)=>{e.subscribe(ue(i,r=>qt(i,t,()=>i.next(r),n),()=>qt(i,t,()=>i.complete(),n),r=>qt(i,t,()=>i.error(r),n)))})}function Td(t,n=0){return me((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function MD(t,n){return Ve(t).pipe(Td(n),Md(n))}function TD(t,n){return Ve(t).pipe(Td(n),Md(n))}function AD(t,n){return new de(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function RD(t,n){return new de(e=>{let i;return qt(e,n,()=>{i=t[Ed](),qt(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){e.error(s);return}o?e.complete():e.next(r)},0,!0)}),()=>ge(i?.return)&&i.return()})}function Ad(t,n){if(!t)throw new Error("Iterable cannot be null");return new de(e=>{qt(e,n,()=>{let i=t[Symbol.asyncIterator]();qt(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function kD(t,n){return Ad(Sd(t),n)}function OD(t,n){if(t!=null){if(Dd(t))return MD(t,n);if(yd(t))return AD(t,n);if(bd(t))return TD(t,n);if(wd(t))return Ad(t,n);if(xd(t))return RD(t,n);if(Id(t))return kD(t,n)}throw Cd(t)}function He(t,n){return n?OD(t,n):Ve(t)}function B(...t){let n=Zn(t);return He(t,n)}function $a(t,n){let e=ge(t)?t:()=>t,i=r=>r.error(e());return new de(n?r=>n.schedule(i,0,r):i)}function eo(t){return!!t&&(t instanceof de||ge(t.lift)&&ge(t.subscribe))}var to=ts(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function ND(t){return t instanceof Date&&!isNaN(t)}function fe(t,n){return me((e,i)=>{let r=0;e.subscribe(ue(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:aR}=Array;function lR(t,n){return aR(n)?t(...n):t(n)}function Rd(t){return fe(n=>lR(t,n))}var{isArray:cR}=Array,{getPrototypeOf:dR,prototype:uR,keys:fR}=Object;function kd(t){if(t.length===1){let n=t[0];if(cR(n))return{args:n,keys:null};if(hR(n)){let e=fR(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function hR(t){return t&&typeof t=="object"&&dR(t)===uR}function Od(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function Kn(...t){let n=Zn(t),e=_d(t),{args:i,keys:r}=kd(t);if(i.length===0)return He([],n);let o=new de(mR(i,n,r?s=>Od(r,s):Qt));return e?o.pipe(Rd(e)):o}function mR(t,n,e=Qt){return i=>{PD(n,()=>{let{length:r}=t,o=new Array(r),s=r,a=r;for(let l=0;l<r;l++)PD(n,()=>{let c=He(t[l],n),u=!1;c.subscribe(ue(i,f=>{o[l]=f,u||(u=!0,a--),a||i.next(e(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function PD(t,n,e){t?qt(e,t,n):n()}function FD(t,n,e,i,r,o,s,a){let l=[],c=0,u=0,f=!1,h=()=>{f&&!l.length&&!c&&n.complete()},m=w=>c<i?p(w):l.push(w),p=w=>{o&&n.next(w),c++;let E=!1;Ve(e(w,u++)).subscribe(ue(n,I=>{r?.(I),o?m(I):n.next(I)},()=>{E=!0},void 0,()=>{if(E)try{for(c--;l.length&&c<i;){let I=l.shift();s?qt(n,s,()=>p(I)):p(I)}h()}catch(I){n.error(I)}}))};return t.subscribe(ue(n,m,()=>{f=!0,h()})),()=>{a?.()}}function Rt(t,n,e=1/0){return ge(n)?Rt((i,r)=>fe((o,s)=>n(i,o,r,s))(Ve(t(i,r))),e):(typeof n=="number"&&(e=n),me((i,r)=>FD(i,r,t,e)))}function or(t=1/0){return Rt(Qt,t)}function LD(){return or(1)}function sr(...t){return LD()(He(t,Zn(t)))}function xn(t){return new de(n=>{Ve(t()).subscribe(n)})}function Ga(...t){let n=_d(t),{args:e,keys:i}=kd(t),r=new de(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),l=s,c=s;for(let u=0;u<s;u++){let f=!1;Ve(e[u]).subscribe(ue(o,h=>{f||(f=!0,c--),a[u]=h},()=>l--,void 0,()=>{(!l||!f)&&(c||o.next(i?Od(i,a):a),o.complete())}))}});return n?r.pipe(Rd(n)):r}function VD(t=0,n,e=DD){let i=-1;return n!=null&&(vd(n)?e=n:i=n),new de(r=>{let o=ND(t)?+t-e.now():t;o<0&&(o=0);let s=0;return e.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function Lt(...t){let n=Zn(t),e=wD(t,1/0),i=t;return i.length?i.length===1?Ve(i[0]):or(e)(He(i,n)):We}function be(t,n){return me((e,i)=>{let r=0;e.subscribe(ue(i,o=>t.call(n,o,r++)&&i.next(o)))})}function jD(t){return me((n,e)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let c=r;r=null,e.next(c)}s&&e.complete()},l=()=>{o=null,s&&e.complete()};n.subscribe(ue(e,c=>{i=!0,r=c,o||Ve(t(c)).subscribe(o=ue(e,a,l))},()=>{s=!0,(!i||!o||o.closed)&&e.complete()}))})}function as(t,n=za){return jD(()=>VD(t,n))}function ar(t){return me((n,e)=>{let i=null,r=!1,o;i=n.subscribe(ue(e,void 0,void 0,s=>{o=Ve(t(s,ar(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function no(t,n){return ge(n)?Rt(t,n,1):Rt(t,1)}function Wa(t,n=za){return me((e,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let c=o;o=null,i.next(c)}};function l(){let c=s+t,u=n.now();if(u<c){r=this.schedule(void 0,c-u),i.add(r);return}a()}e.subscribe(ue(i,c=>{o=c,s=n.now(),r||(r=n.schedule(l,t),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function BD(t){return me((n,e)=>{let i=!1;n.subscribe(ue(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function ze(t){return t<=0?()=>We:me((n,e)=>{let i=0;n.subscribe(ue(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function Nd(t,n=Qt){return t=t??pR,me((e,i)=>{let r,o=!0;e.subscribe(ue(i,s=>{let a=n(s);(o||!t(r,a))&&(o=!1,r=a,i.next(s))}))})}function pR(t,n){return t===n}function UD(t=gR){return me((n,e)=>{let i=!1;n.subscribe(ue(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function gR(){return new to}function io(t){return me((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function xi(t,n){let e=arguments.length>=2;return i=>i.pipe(t?be((r,o)=>t(r,o,i)):Qt,ze(1),e?BD(n):UD(()=>new to))}function Pd(t){return t<=0?()=>We:me((n,e)=>{let i=[];n.subscribe(ue(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function Fd(){return me((t,n)=>{let e,i=!1;t.subscribe(ue(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function qa(t={}){let{connector:n=()=>new x,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let s,a,l,c=0,u=!1,f=!1,h=()=>{a?.unsubscribe(),a=void 0},m=()=>{h(),s=l=void 0,u=f=!1},p=()=>{let w=s;m(),w?.unsubscribe()};return me((w,E)=>{c++,!f&&!u&&h();let I=l=l??n();E.add(()=>{c--,c===0&&!f&&!u&&(a=tp(p,r))}),I.subscribe(E),!s&&c>0&&(s=new Ei({next:ce=>I.next(ce),error:ce=>{f=!0,h(),a=tp(m,e,ce),I.error(ce)},complete:()=>{u=!0,h(),a=tp(m,i),I.complete()}}),Ve(w).subscribe(s))})(o)}}function tp(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new Ei({next:()=>{i.unsubscribe(),t()}});return Ve(n(...e)).subscribe(i)}function Ld(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,qa({connector:()=>new nr(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function Ya(t){return be((n,e)=>t<=e)}function Je(...t){let n=Zn(t);return me((e,i)=>{(n?sr(t,e,n):sr(t,e)).subscribe(i)})}function ot(t,n){return me((e,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();e.subscribe(ue(i,l=>{r?.unsubscribe();let c=0,u=o++;Ve(t(l,u)).subscribe(r=ue(i,f=>i.next(n?n(l,f,u,c++):f),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function Ae(t){return me((n,e)=>{Ve(t).subscribe(ue(e,()=>e.complete(),Va)),!e.closed&&n.subscribe(e)})}function np(t,n=!1){return me((e,i)=>{let r=0;e.subscribe(ue(i,o=>{let s=t(o,r++);(s||n)&&i.next(o),!s&&i.complete()}))})}function Dt(t,n,e){let i=ge(t)||n||e?{next:t,error:n,complete:e}:t;return i?me((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(ue(o,l=>{var c;(c=i.next)===null||c===void 0||c.call(i,l),o.next(l)},()=>{var l;a=!1,(l=i.complete)===null||l===void 0||l.call(i),o.complete()},l=>{var c;a=!1,(c=i.error)===null||c===void 0||c.call(i,l),o.error(l)},()=>{var l,c;a&&((l=i.unsubscribe)===null||l===void 0||l.call(i)),(c=i.finalize)===null||c===void 0||c.call(i)}))}):Qt}var ip;function Vd(){return ip}function Qn(t){let n=ip;return ip=t,n}var HD=Symbol("NotFound");function ls(t){return t===HD||t?.name==="\u0275NotFound"}function rp(t,n,e){let i=Object.create(vR);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(Yr(i),Ji(i),i.value===Yn)throw i.error;return i.value};return o[it]=i,Na(i),o}function zD(t,n){Yr(t),Zr(t,n),Xo(t)}function $D(t,n){if(Yr(t),t.value===Yn)throw t.error;rd(t,n),Xo(t)}var vR=te(y({},Xi),{value:Wr,dirty:!0,error:null,equal:Pa,kind:"linkedSignal",producerMustRecompute(t){return t.value===Wr||t.value===qr},producerRecomputeValue(t){if(t.value===qr)throw new Error("");let n=t.value;t.value=qr;let e=Ci(t),i,r=!1;try{let o=t.source(),s=n!==Wr&&n!==Yn,a=s?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,a),t.sourceValue=o,X(null),r=s&&i!==Yn&&t.equal(n,i)}catch(o){i=Yn,t.error=o}finally{er(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function GD(t){let n=X(null);try{return t()}finally{X(n)}}var Gd="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",S=class extends Error{code;constructor(n,e){super(Xn(n,e)),this.code=n}};function _R(t){return`NG0${Math.abs(t)}`}function Xn(t,n){return`${_R(t)}${n?": "+n:""}`}var fn=globalThis;function Oe(t){for(let n in t)if(t[n]===Oe)return n;throw Error("")}function KD(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function tl(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(tl).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function Wd(t,n){return t?n?`${t} ${n}`:t:n||""}var yR=Oe({__forward_ref__:Oe});function Yt(t){return t.__forward_ref__=Yt,t}function gt(t){return vp(t)?t():t}function vp(t){return typeof t=="function"&&t.hasOwnProperty(yR)&&t.__forward_ref__===Yt}function b(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function A(t){return{providers:t.providers||[],imports:t.imports||[]}}function nl(t){return bR(t,qd)}function _p(t){return nl(t)!==null}function bR(t,n){return t.hasOwnProperty(n)&&t[n]||null}function DR(t){let n=t?.[qd]??null;return n||null}function sp(t){return t&&t.hasOwnProperty(Bd)?t[Bd]:null}var qd=Oe({\u0275prov:Oe}),Bd=Oe({\u0275inj:Oe}),g=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=b({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function yp(t){return t&&!!t.\u0275providers}var bp=Oe({\u0275cmp:Oe}),Dp=Oe({\u0275dir:Oe}),wp=Oe({\u0275pipe:Oe}),Cp=Oe({\u0275mod:Oe}),Ka=Oe({\u0275fac:Oe}),lo=Oe({__NG_ELEMENT_ID__:Oe}),WD=Oe({__NG_ENV_ID__:Oe});function Ep(t){return Yd(t,"@NgModule"),t[Cp]||null}function In(t){return Yd(t,"@Component"),t[bp]||null}function il(t){return Yd(t,"@Directive"),t[Dp]||null}function xp(t){return Yd(t,"@Pipe"),t[wp]||null}function Yd(t,n){if(t==null)throw new S(-919,!1)}function ds(t){return typeof t=="string"?t:t==null?"":String(t)}var QD=Oe({ngErrorCode:Oe}),wR=Oe({ngErrorMessage:Oe}),CR=Oe({ngTokenPath:Oe});function Sp(t,n){return XD("",-200,n)}function Zd(t,n){throw new S(-201,!1)}function XD(t,n,e){let i=new S(n,t);return i[QD]=n,i[wR]=t,e&&(i[CR]=e),i}function ER(t){return t[QD]}var ap;function JD(){return ap}function Vt(t){let n=ap;return ap=t,n}function Ip(t,n,e){let i=nl(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;Zd(t,"")}var xR={},ro=xR,SR="__NG_DI_FLAG__",lp=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=oo(e)||0;try{return this.injector.get(n,i&8?null:ro,i)}catch(r){if(ls(r))return r;throw r}}};function IR(t,n=0){let e=Vd();if(e===void 0)throw new S(-203,!1);if(e===null)return Ip(t,void 0,n);{let i=MR(n),r=e.retrieve(t,i);if(ls(r)){if(i.optional)return null;throw r}return r}}function K(t,n=0){return(JD()||IR)(gt(t),n)}function d(t,n){return K(t,oo(n))}function oo(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function MR(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function cp(t){let n=[];for(let e=0;e<t.length;e++){let i=gt(t[e]);if(Array.isArray(i)){if(i.length===0)throw new S(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],l=TR(a);typeof l=="number"?l===-1?r=a.token:o|=l:r=a}n.push(K(r,o))}else n.push(K(i))}return n}function TR(t){return t[SR]}function lr(t,n){let e=t.hasOwnProperty(Ka);return e?t[Ka]:null}function ew(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function tw(t){return t.flat(Number.POSITIVE_INFINITY)}function Kd(t,n){t.forEach(e=>Array.isArray(e)?Kd(e,n):n(e))}function Mp(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function rl(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function nw(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function iw(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function Qd(t,n,e){let i=us(t,n);return i>=0?t[i|1]=e:(i=~i,iw(t,i,n,e)),i}function Xd(t,n){let e=us(t,n);if(e>=0)return t[e|1]}function us(t,n){return AR(t,n,1)}function AR(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),s=t[o<<e];if(n===s)return o<<e;s>n?r=o:i=o+1}return~(r<<e)}var Mn={},kt=[],Jn=new g(""),Tp=new g("",-1),Ap=new g(""),Qa=class{get(n,e=ro){if(e===ro){let r=XD("",-201);throw r.name="\u0275NotFound",r}return e}};function ei(t){return{\u0275providers:t}}function rw(t){return ei([{provide:Jn,multi:!0,useValue:t}])}function ow(...t){return{\u0275providers:Jd(!0,t),\u0275fromNgModule:!0}}function Jd(t,...n){let e=[],i=new Set,r,o=s=>{e.push(s)};return Kd(n,s=>{let a=s;Ud(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&sw(r,o),e}function sw(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];Rp(r,o=>{n(o,i)})}}function Ud(t,n,e,i){if(t=gt(t),!t)return!1;let r=null,o=sp(t),s=!o&&In(t);if(!o&&!s){let l=t.ngModule;if(o=sp(l),o)r=l;else return!1}else{if(s&&!s.standalone)return!1;r=t}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let l=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let c of l)Ud(c,n,e,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let c;Kd(o.imports,u=>{Ud(u,n,e,i)&&(c||=[],c.push(u))}),c!==void 0&&sw(c,n)}if(!a){let c=lr(r)||(()=>new r);n({provide:r,useFactory:c,deps:kt},r),n({provide:Ap,useValue:r,multi:!0},r),n({provide:Jn,useValue:()=>K(r),multi:!0},r)}let l=o.providers;if(l!=null&&!a){let c=t;Rp(l,u=>{n(u,c)})}}else return!1;return r!==t&&t.providers!==void 0}function Rp(t,n){for(let e of t)yp(e)&&(e=e.\u0275providers),Array.isArray(e)?Rp(e,n):n(e)}var RR=Oe({provide:String,useValue:Oe});function aw(t){return t!==null&&typeof t=="object"&&RR in t}function kR(t){return!!(t&&t.useExisting)}function OR(t){return!!(t&&t.useFactory)}function so(t){return typeof t=="function"}function lw(t){return!!t.useClass}var ol=new g(""),jd={},qD={},op;function fs(){return op===void 0&&(op=new Qa),op}var Ne=class{},ao=class extends Ne{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,up(n,s=>this.processProvider(s)),this.records.set(Tp,cs(void 0,this)),r.has("environment")&&this.records.set(Ne,cs(void 0,this));let o=this.records.get(ol);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Ap,kt,{self:!0}))}retrieve(n,e){let i=oo(e)||0;try{return this.get(n,ro,i)}catch(r){if(ls(r))return r;throw r}}destroy(){Za(this),this._destroyed=!0;let n=X(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),X(n)}}onDestroy(n){return Za(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){Za(this);let e=Qn(this),i=Vt(void 0),r;try{return n()}finally{Qn(e),Vt(i)}}get(n,e=ro,i){if(Za(this),n.hasOwnProperty(WD))return n[WD](this);let r=oo(i),o,s=Qn(this),a=Vt(void 0);try{if(!(r&4)){let c=this.records.get(n);if(c===void 0){let u=VR(n)&&nl(n);u&&this.injectableDefInScope(u)?c=cs(dp(n),jd):c=null,this.records.set(n,c)}if(c!=null)return this.hydrate(n,c,r)}let l=r&2?fs():this.parent;return e=r&8&&e===ro?null:e,l.get(n,e)}catch(l){let c=ER(l);throw c===-200||c===-201?new S(c,null):l}finally{Vt(a),Qn(s)}}resolveInjectorInitializers(){let n=X(null),e=Qn(this),i=Vt(void 0),r;try{let o=this.get(Jn,kt,{self:!0});for(let s of o)s()}finally{Qn(e),Vt(i),X(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=gt(n);let e=so(n)?n:gt(n&&n.provide),i=PR(n);if(!so(n)&&n.multi===!0){let r=this.records.get(e);r||(r=cs(void 0,jd,!0),r.factory=()=>cp(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=X(null);try{if(e.value===qD)throw Sp("");return e.value===jd&&(e.value=qD,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&LR(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{X(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=gt(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function dp(t){let n=nl(t),e=n!==null?n.factory:lr(t);if(e!==null)return e;if(t instanceof g)throw new S(-204,!1);if(t instanceof Function)return NR(t);throw new S(-204,!1)}function NR(t){if(t.length>0)throw new S(-204,!1);let e=DR(t);return e!==null?()=>e.factory(t):()=>new t}function PR(t){if(aw(t))return cs(void 0,t.useValue);{let n=kp(t);return cs(n,jd)}}function kp(t,n,e){let i;if(so(t)){let r=gt(t);return lr(r)||dp(r)}else if(aw(t))i=()=>gt(t.useValue);else if(OR(t))i=()=>t.useFactory(...cp(t.deps||[]));else if(kR(t))i=(r,o)=>K(gt(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=gt(t&&(t.useClass||t.provide));if(FR(t))i=()=>new r(...cp(t.deps));else return lr(r)||dp(r)}return i}function Za(t){if(t.destroyed)throw new S(-205,!1)}function cs(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function FR(t){return!!t.deps}function LR(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function VR(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function up(t,n){for(let e of t)Array.isArray(e)?up(e,n):e&&yp(e)?up(e.\u0275providers,n):n(e)}function wt(t,n){let e;t instanceof ao?(Za(t),e=t):e=new lp(t);let i,r=Qn(e),o=Vt(void 0);try{return n()}finally{Qn(r),Vt(o)}}function cw(){return JD()!==void 0||Vd()!=null}var Bt=0,U=1,se=2,st=3,hn=4,Ut=5,Ht=6,hs=7,at=8,Xt=9,Tn=10,Ee=11,ms=12,Op=13,co=14,St=15,ur=16,uo=17,ti=18,Ii=19,Np=20,Si=21,eu=22,cr=23,Jt=24,fo=25,fr=26,xe=27,dw=1,An=6,ni=7,sl=8,ho=9,qe=10;function mn(t){return Array.isArray(t)&&typeof t[dw]=="object"}function en(t){return Array.isArray(t)&&t[dw]===!0}function Pp(t){return(t.flags&4)!==0}function ii(t){return t.componentOffset>-1}function ps(t){return(t.flags&1)===1}function Rn(t){return!!t.template}function mo(t){return(t[se]&512)!==0}function hr(t){return(t[se]&256)===256}var Fp="svg",uw="math";function tn(t){for(;Array.isArray(t);)t=t[Bt];return t}function Lp(t,n){return tn(n[t])}function pn(t,n){return tn(n[t.index])}function gs(t,n){return t.data[n]}function Vp(t,n){return t[n]}function jp(t,n,e,i){e>=t.data.length&&(t.data[e]=null,t.blueprint[e]=null),n[e]=i}function gn(t,n){let e=n[t];return mn(e)?e:e[Bt]}function fw(t){return(t[se]&4)===4}function tu(t){return(t[se]&128)===128}function hw(t){return en(t[st])}function nn(t,n){return n==null?null:t[n]}function Bp(t){t[uo]=0}function Up(t){t[se]&1024||(t[se]|=1024,tu(t)&&po(t))}function mw(t,n){for(;t>0;)n=n[co],t--;return n}function al(t){return!!(t[se]&9216||t[Jt]?.dirty)}function nu(t){t[Tn].changeDetectionScheduler?.notify(8),t[se]&64&&(t[se]|=1024),al(t)&&po(t)}function po(t){t[Tn].changeDetectionScheduler?.notify(0);let n=dr(t);for(;n!==null&&!(n[se]&8192||(n[se]|=8192,!tu(n)));)n=dr(n)}function Hp(t,n){if(hr(t))throw new S(911,!1);t[Si]===null&&(t[Si]=[]),t[Si].push(n)}function pw(t,n){if(t[Si]===null)return;let e=t[Si].indexOf(n);e!==-1&&t[Si].splice(e,1)}function dr(t){let n=t[st];return en(n)?n[st]:n}function zp(t){return t[hs]??=[]}function $p(t){return t.cleanup??=[]}function gw(t,n,e,i){let r=zp(n);r.push(e),t.firstCreatePass&&$p(t).push(i,r.length-1)}var ve={lFrame:Tw(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var fp=!1;function vw(){return ve.lFrame.elementDepthCount}function _w(){ve.lFrame.elementDepthCount++}function Gp(){ve.lFrame.elementDepthCount--}function iu(){return ve.bindingsEnabled}function ru(){return ve.skipHydrationRootTNode!==null}function Wp(t){return ve.skipHydrationRootTNode===t}function yw(t){ve.skipHydrationRootTNode=t}function qp(){ve.skipHydrationRootTNode=null}function ie(){return ve.lFrame.lView}function Ue(){return ve.lFrame.tView}function Ye(t){return ve.lFrame.contextLView=t,t[at]}function Ze(t){return ve.lFrame.contextLView=null,t}function vt(){let t=Yp();for(;t!==null&&t.type===64;)t=t.parent;return t}function Yp(){return ve.lFrame.currentTNode}function bw(){let t=ve.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function vs(t,n){let e=ve.lFrame;e.currentTNode=t,e.isParent=n}function Zp(){return ve.lFrame.isParent}function Kp(){ve.lFrame.isParent=!1}function Dw(){return ve.lFrame.contextLView}function Qp(){return fp}function Xa(t){let n=fp;return fp=t,n}function Xp(){let t=ve.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function ww(){return ve.lFrame.bindingIndex}function Cw(t){return ve.lFrame.bindingIndex=t}function mr(){return ve.lFrame.bindingIndex++}function ou(t){let n=ve.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function Ew(){return ve.lFrame.inI18n}function xw(t,n){let e=ve.lFrame;e.bindingIndex=e.bindingRootIndex=t,su(n)}function Sw(){return ve.lFrame.currentDirectiveIndex}function su(t){ve.lFrame.currentDirectiveIndex=t}function Iw(t){let n=ve.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function au(){return ve.lFrame.currentQueryIndex}function ll(t){ve.lFrame.currentQueryIndex=t}function jR(t){let n=t[U];return n.type===2?n.declTNode:n.type===1?t[Ut]:null}function Jp(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=jR(o),r===null||(o=o[co],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=ve.lFrame=Mw();return i.currentTNode=n,i.lView=t,!0}function lu(t){let n=Mw(),e=t[U];ve.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function Mw(){let t=ve.lFrame,n=t===null?null:t.child;return n===null?Tw(t):n}function Tw(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function Aw(){let t=ve.lFrame;return ve.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var eg=Aw;function cu(){let t=Aw();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function Rw(t){return(ve.lFrame.contextLView=mw(t,ve.lFrame.contextLView))[at]}function ri(){return ve.lFrame.selectedIndex}function pr(t){ve.lFrame.selectedIndex=t}function cl(){let t=ve.lFrame;return gs(t.tView,t.selectedIndex)}function oi(){ve.lFrame.currentNamespace=Fp}function du(){return ve.lFrame.currentNamespace}var kw=!0;function uu(){return kw}function Mi(t){kw=t}function hp(t,n=null,e=null,i){let r=tg(t,n,e,i);return r.resolveInjectorInitializers(),r}function tg(t,n=null,e=null,i,r=new Set){let o=[e||kt,ow(t)],s;return new ao(o,n||fs(),s||null,r)}var W=class t{static THROW_IF_NOT_FOUND=ro;static NULL=new Qa;static create(n,e){if(Array.isArray(n))return hp({name:""},e,n,"");{let i=n.name??"";return hp({name:i},n.parent,n.providers,i)}}static \u0275prov=b({token:t,providedIn:"any",factory:()=>K(Tp)});static __NG_ELEMENT_ID__=-1},P=new g(""),zt=(()=>{class t{static __NG_ELEMENT_ID__=BR;static __NG_ENV_ID__=e=>e}return t})(),Hd=class extends zt{_lView;constructor(n){super(),this._lView=n}get destroyed(){return hr(this._lView)}onDestroy(n){let e=this._lView;return Hp(e,n),()=>pw(e,n)}};function BR(){return new Hd(ie())}var Ow=!1,Nw=new g(""),si=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new rt(!1);debugTaskTracker=d(Nw,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new de(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=b({token:t,providedIn:"root",factory:()=>new t})}return t})(),mp=class extends x{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,cw()&&(this.destroyRef=d(zt,{optional:!0})??void 0,this.pendingTasks=d(si,{optional:!0})??void 0)}emit(n){let e=X(null);try{super.next(n)}finally{X(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),s=i;if(n&&typeof n=="object"){let l=n;r=l.next?.bind(l),o=l.error?.bind(l),s=l.complete?.bind(l)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof ae&&n.add(a),a}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},j=mp;function zd(...t){}function ng(t){let n,e;function i(){t=zd;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function Pw(t){return queueMicrotask(()=>t()),()=>{t=zd}}var ig="isAngularZone",Ja=ig+"_ID",UR=0,G=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new j(!1);onMicrotaskEmpty=new j(!1);onStable=new j(!1);onError=new j(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=Ow}=n;if(typeof Zone>"u")throw new S(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,$R(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(ig)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new S(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new S(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,HR,zd,zd);try{return o.runTask(s,e,i)}finally{o.cancelTask(s)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},HR={};function rg(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function zR(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){ng(()=>{t.callbackScheduled=!1,pp(t),t.isCheckStableRunning=!0,rg(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),pp(t)}function $R(t){let n=()=>{zR(t)},e=UR++;t._inner=t._inner.fork({name:"angular",properties:{[ig]:!0,[Ja]:e,[Ja+e]:!0},onInvokeTask:(i,r,o,s,a,l)=>{if(GR(l))return i.invokeTask(o,s,a,l);try{return YD(t),i.invokeTask(o,s,a,l)}finally{(t.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),ZD(t)}},onInvoke:(i,r,o,s,a,l,c)=>{try{return YD(t),i.invoke(o,s,a,l,c)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!WR(l)&&n(),ZD(t)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(t._hasPendingMicrotasks=s.microTask,pp(t),rg(t)):s.change=="macroTask"&&(t.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),t.runOutsideAngular(()=>t.onError.emit(s)),!1)})}function pp(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function YD(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function ZD(t){t._nesting--,rg(t)}var el=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new j;onMicrotaskEmpty=new j;onStable=new j;onError=new j;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function GR(t){return Fw(t,"__ignore_ng_zone__")}function WR(t){return Fw(t,"__scheduler_tick__")}function Fw(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var jt=class{_console=console;handleError(n){this._console.error("ERROR",n)}},rn=new g("",{factory:()=>{let t=d(G),n=d(Ne),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(jt),e.handleError(i))})}}}),Lw={provide:Jn,useValue:()=>{let t=d(jt,{optional:!0})},multi:!0},qR=new g("",{factory:()=>{let t=d(P).defaultView;if(!t)return;let n=d(rn),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),d(zt).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function og(){return ei([rw(()=>{d(qR)})])}function k(t,n){let[e,i,r]=Bm(t,n?.equal),o=e,s=o[it];return o.set=i,o.update=r,o.asReadonly=fu.bind(o),o}function fu(){let t=this[it];if(t.readonlyFn===void 0){let n=()=>this();n[it]=t,t.readonlyFn=n}return t.readonlyFn}var _s=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=YR}return t})();function YR(){return new _s(ie(),vt())}var Sn=class{},dl=new g("",{factory:()=>!0});var sg=new g(""),ys=(()=>{class t{internalPendingTasks=d(si);scheduler=d(Sn);errorHandler=d(rn);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();e().catch(this.errorHandler).finally(i)}static \u0275prov=b({token:t,providedIn:"root",factory:()=>new t})}return t})(),hu=(()=>{class t{static \u0275prov=b({token:t,providedIn:"root",factory:()=>new gp})}return t})(),gp=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},$d=class{[it];constructor(n){this[it]=n}destroy(){this[it].destroy()}};function kn(t,n){let e=n?.injector??d(W),i=n?.manualCleanup!==!0?e.get(zt):null,r,o=e.get(_s,null,{optional:!0}),s=e.get(Sn);return o!==null?(r=QR(o.view,s,t),i instanceof Hd&&i._lView===o.view&&(i=null)):r=XR(t,e.get(hu),s),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new $d(r)}var Vw=te(y({},Um),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=Xa(!1);try{Hm(this)}finally{Xa(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=X(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],X(t)}}}),ZR=te(y({},Vw),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(tr(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),KR=te(y({},Vw),{consumerMarkedDirty(){this.view[se]|=8192,po(this.view),this.notifier.notify(13)},destroy(){if(tr(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[cr]?.delete(this)}});function QR(t,n,e){let i=Object.create(KR);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=jw(i,e),t[cr]??=new Set,t[cr].add(i),i.consumerMarkedDirty(i),i}function XR(t,n,e){let i=Object.create(ZR);return i.fn=jw(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function jw(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}var mu={JSACTION:"jsaction"};function Cl(t){return{toString:t}.toString()}function sk(t){return typeof t=="function"}function RC(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var Su=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}},Ie=(()=>{let t=()=>kC;return t.ngInherit=!0,t})();function kC(t){return t.type.prototype.ngOnChanges&&(t.setInput=lk),ak}function ak(){let t=NC(this),n=t?.current;if(n){let e=t.previous;if(e===Mn)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function lk(t,n,e,i,r){let o=this.declaredInputs[i],s=NC(t)||ck(t,{previous:Mn,current:null}),a=s.current||(s.current={}),l=s.previous,c=l[o];a[o]=new Su(c&&c.currentValue,e,l===Mn),RC(t,n,r,e)}var OC="__ngSimpleChanges__";function NC(t){return t[OC]||null}function ck(t,n){return t[OC]=n}var Bw=[];var Se=function(t,n=null,e){for(let i=0;i<Bw.length;i++){let r=Bw[i];r(t,n,e)}},we=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(we||{});function dk(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=kC(n);(e.preOrderHooks??=[]).push(t,s),(e.preOrderCheckHooks??=[]).push(t,s)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function PC(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:u}=o;s&&(t.contentHooks??=[]).push(-e,s),a&&((t.contentHooks??=[]).push(e,a),(t.contentCheckHooks??=[]).push(e,a)),l&&(t.viewHooks??=[]).push(-e,l),c&&((t.viewHooks??=[]).push(e,c),(t.viewCheckHooks??=[]).push(e,c)),u!=null&&(t.destroyHooks??=[]).push(e,u)}}function yu(t,n,e){FC(t,n,3,e)}function bu(t,n,e,i){(t[se]&3)===e&&FC(t,n,e,i)}function ag(t,n){let e=t[se];(e&3)===n&&(e&=16383,e+=1,t[se]=e)}function FC(t,n,e,i){let r=i!==void 0?t[uo]&65535:0,o=i??-1,s=n.length-1,a=0;for(let l=r;l<s;l++)if(typeof n[l+1]=="number"){if(a=n[l],i!=null&&a>=i)break}else n[l]<0&&(t[uo]+=65536),(a<o||o==-1)&&(uk(t,e,n,l),t[uo]=(t[uo]&4294901760)+l+2),l++}function Uw(t,n){Se(we.LifecycleHookStart,t,n);let e=X(null);try{n.call(t)}finally{X(e),Se(we.LifecycleHookEnd,t,n)}}function uk(t,n,e,i){let r=e[i]<0,o=e[i+1],s=r?-e[i]:e[i],a=t[s];r?t[se]>>14<t[uo]>>16&&(t[se]&3)===n&&(t[se]+=16384,Uw(a,o)):Uw(a,o)}var Ds=-1,_o=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function fk(t){return(t.flags&8)!==0}function hk(t){return(t.flags&16)!==0}function mk(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],s=e[i++],a=e[i++];t.setAttribute(n,s,a,o)}else{let o=r,s=e[++i];pk(o)?t.setProperty(n,o,s):t.setAttribute(n,o,s),i++}}return i}function LC(t){return t===3||t===4||t===6}function pk(t){return t.charCodeAt(0)===64}function Es(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?Hw(t,e,r,null,n[++i]):Hw(t,e,r,null,null))}}return t}function Hw(t,n,e,i,r){let o=0,s=t.length;if(n===-1)s=-1;else for(;o<t.length;){let a=t[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<t.length;){let a=t[o];if(typeof a=="number")break;if(a===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(t.splice(s,0,n),o=s+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function VC(t){return t!==Ds}function Iu(t){return t&32767}function gk(t){return t>>16}function Mu(t,n){let e=gk(t),i=n;for(;e>0;)i=i[co],e--;return i}var Dg=!0;function Tu(t){let n=Dg;return Dg=t,n}var vk=256,jC=vk-1,BC=5,_k=0,ai={};function yk(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(lo)&&(i=e[lo]),i==null&&(i=e[lo]=_k++);let r=i&jC,o=1<<r;n.data[t+(r>>BC)]|=o}function Au(t,n){let e=UC(t,n);if(e!==-1)return e;let i=n[U];i.firstCreatePass&&(t.injectorIndex=n.length,lg(i.data,t),lg(n,null),lg(i.blueprint,null));let r=av(t,n),o=t.injectorIndex;if(VC(r)){let s=Iu(r),a=Mu(r,n),l=a[U].data;for(let c=0;c<8;c++)n[o+c]=a[s+c]|l[s+c]}return n[o+8]=r,o}function lg(t,n){t.push(0,0,0,0,0,0,0,0,n)}function UC(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function av(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=WC(r),i===null)return Ds;if(e++,r=r[co],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Ds}function wg(t,n,e){yk(t,n,e)}function bk(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(LC(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function HC(t,n,e){if(e&8||t!==void 0)return t;Zd(n,"NodeInjector")}function zC(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[Xt],o=Vt(void 0);try{return r?r.get(n,i,e&8):Ip(n,i,e&8)}finally{Vt(o)}}return HC(i,n,e)}function $C(t,n,e,i=0,r){if(t!==null){if(n[se]&2048&&!(i&2)){let s=Ek(t,n,e,i,ai);if(s!==ai)return s}let o=GC(t,n,e,i,ai);if(o!==ai)return o}return zC(n,e,i,r)}function GC(t,n,e,i,r){let o=wk(e);if(typeof o=="function"){if(!Jp(n,t,i))return i&1?HC(r,e,i):zC(n,e,i,r);try{let s;if(s=o(i),s==null&&!(i&8))Zd(e);else return s}finally{eg()}}else if(typeof o=="number"){let s=null,a=UC(t,n),l=Ds,c=i&1?n[St][Ut]:null;for((a===-1||i&4)&&(l=a===-1?av(t,n):n[a+8],l===Ds||!$w(i,!1)?a=-1:(s=n[U],a=Iu(l),n=Mu(l,n)));a!==-1;){let u=n[U];if(zw(o,a,u.data)){let f=Dk(a,n,e,s,i,c);if(f!==ai)return f}l=n[a+8],l!==Ds&&$w(i,n[U].data[a+8]===c)&&zw(o,a,n)?(s=u,a=Iu(l),n=Mu(l,n)):a=-1}}return r}function Dk(t,n,e,i,r,o){let s=n[U],a=s.data[t+8],l=i==null?ii(a)&&Dg:i!=s&&(a.type&3)!==0,c=r&1&&o===a,u=Du(a,s,e,l,c);return u!==null?pl(n,s,u,a,r):ai}function Du(t,n,e,i,r){let o=t.providerIndexes,s=n.data,a=o&1048575,l=t.directiveStart,c=t.directiveEnd,u=o>>20,f=i?a:a+u,h=r?a+u:c;for(let m=f;m<h;m++){let p=s[m];if(m<l&&e===p||m>=l&&p.type===e)return m}if(r){let m=s[l];if(m&&Rn(m)&&m.type===e)return l}return null}function pl(t,n,e,i,r){let o=t[e],s=n.data;if(o instanceof _o){let a=o;if(a.resolving)throw Sp("");let l=Tu(a.canSeeViewProviders);a.resolving=!0;let c=s[e].type||s[e],u,f=a.injectImpl?Vt(a.injectImpl):null,h=Jp(t,i,0);try{o=t[e]=a.factory(void 0,r,s,t,i),n.firstCreatePass&&e>=i.directiveStart&&dk(e,s[e],n)}finally{f!==null&&Vt(f),Tu(l),a.resolving=!1,eg()}}return o}function wk(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(lo)?t[lo]:void 0;return typeof n=="number"?n>=0?n&jC:Ck:n}function zw(t,n,e){let i=1<<t;return!!(e[n+(t>>BC)]&i)}function $w(t,n){return!(t&2)&&!(t&1&&n)}var vo=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return $C(this._tNode,this._lView,n,oo(i),e)}};function Ck(){return new vo(vt(),ie())}function Le(t){return Cl(()=>{let n=t.prototype.constructor,e=n[Ka]||Cg(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[Ka]||Cg(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function Cg(t){return vp(t)?()=>{let n=Cg(gt(t));return n&&n()}:lr(t)}function Ek(t,n,e,i,r){let o=t,s=n;for(;o!==null&&s!==null&&s[se]&2048&&!mo(s);){let a=GC(o,s,e,i|2,ai);if(a!==ai)return a;let l=o.parent;if(!l){let c=s[Np];if(c){let u=c.get(e,ai,i&-5);if(u!==ai)return u}l=WC(s),s=s[co]}o=l}return r}function WC(t){let n=t[U],e=n.type;return e===2?n.declTNode:e===1?t[Ut]:null}function El(t){return bk(vt(),t)}function xk(){return Ms(vt(),ie())}function Ms(t,n){return new O(pn(t,n))}var O=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=xk}return t})();function qC(t){return t instanceof O?t.nativeElement:t}function Sk(){return this._results[Symbol.iterator]()}var Pn=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new x}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=tw(n);(this._changesDetected=!ew(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=Sk},YC="ngSkipHydration",Ik="ngskiphydration";function ZC(t){let n=t.mergedAttrs;if(n===null)return!1;for(let e=0;e<n.length;e+=2){let i=n[e];if(typeof i=="number")return!1;if(typeof i=="string"&&i.toLowerCase()===Ik)return!0}return!1}function KC(t){return t.hasAttribute(YC)}function Ru(t){return(t.flags&128)===128}function QC(t){if(Ru(t))return!0;let n=t.parent;for(;n;){if(Ru(t)||ZC(n))return!0;n=n.parent}return!1}var lv=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(lv||{}),XC=new Map,Mk=0;function Tk(){return Mk++}function Ak(t){XC.set(t[Ii],t)}function Eg(t){XC.delete(t[Ii])}var Gw="__ngContext__";function xs(t,n){mn(n)?(t[Gw]=n[Ii],Ak(n)):t[Gw]=n}function JC(t){return t0(t[ms])}function e0(t){return t0(t[hn])}function t0(t){for(;t!==null&&!en(t);)t=t[hn];return t}var xg;function cv(t){xg=t}function n0(){if(xg!==void 0)return xg;if(typeof document<"u")return document;throw new S(210,!1)}var _n=new g("",{factory:()=>Rk}),Rk="ng";var $u=new g(""),Ai=new g("",{providedIn:"platform",factory:()=>"unknown"}),xl=new g(""),wo=new g("",{factory:()=>d(P).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Ts=(()=>{class t{static \u0275prov=b({token:t,providedIn:"root",factory:()=>{let e=new t;return e.store=kk(d(P),d(_n)),e}});store={};onSerializeCallbacks={};get(e,i){return this.store[e]!==void 0?this.store[e]:i}set(e,i){this.store[e]=i}remove(e){delete this.store[e]}hasKey(e){return this.store.hasOwnProperty(e)}get isEmpty(){return Object.keys(this.store).length===0}onSerialize(e,i){this.onSerializeCallbacks[e]=i}toJson(){for(let e in this.onSerializeCallbacks)if(this.onSerializeCallbacks.hasOwnProperty(e))try{this.store[e]=this.onSerializeCallbacks[e]()}catch(i){console.warn("Exception in onSerialize callback: ",i)}return JSON.stringify(this.store).replace(/</g,"\\u003C").replace(/\//g,"\\u002F")}}return t})();function kk(t,n){let e=t.getElementById(n+"-state");if(e?.textContent)try{return JSON.parse(e.textContent)}catch(i){console.warn("Exception while restoring TransferState for app "+n,i)}return{}}var i0="h",r0="b",Ok="f",Nk="n",o0="e",s0="t",Gu="c",dv="x",gl="r",a0="i",l0="n",uv="d";var c0="di",d0="s",u0="p";var As=new g(""),f0=!1,fv=new g("",{factory:()=>f0});var hv=new g(""),h0=!1,m0=new g("",{factory:()=>[]}),p0=new g(""),mv=new g("",{factory:()=>new Map});var Sl="ngb";var g0=(t,n,e)=>{let i=t,r=i.__jsaction_fns??new Map,o=r.get(n)??[];o.push(e),r.set(n,o),i.__jsaction_fns=r},v0=(t,n)=>{let e=t,i=e.getAttribute(Sl)??"",r=n.get(i)??new Set;r.has(e)||r.add(e),n.set(i,r)};var _0=t=>{t.removeAttribute(mu.JSACTION),t.removeAttribute(Sl),t.__jsaction_fns=void 0},y0=new g("",{factory:()=>({})}),Sg=new WeakMap;function Pk(t,n){if(t==null||typeof t!="object")return;let e=Sg.get(t);e||(e=new WeakSet,Sg.set(t,e)),e.add(n)}function pv(t,n){let e=n?.__jsaction_fns?.get(t.type);if(!(!e||!n?.isConnected)&&!(n&&Sg.get(t)?.has(n)))for(let i of e)i(t)}var Ig=new Map;function b0(t,n){return Ig.set(t,n),()=>Ig.delete(t)}var Ww=!1,D0=(t,n,e,i)=>{};function Fk(t,n,e,i){D0(t,n,e,i)}function w0(){Ww||(D0=(t,n,e,i)=>{let r=t[Xt].get(_n);Ig.get(r)?.(n,e,i)},Ww=!0)}var Wu=new g("");function Il(t){return(t.flags&32)===32}var Lk="__nghData__",gv=Lk,Vk="__nghDeferData__",C0=Vk;var wu="ngh",E0="nghm",x0=()=>null;function jk(t,n,e=!1){let i=t.getAttribute(wu);if(i==null)return null;let[r,o]=i.split("|");if(i=e?o:r,!i)return null;let s=o?`|${o}`:"",a=e?r:s,l={};if(i!==""){let u=n.get(Ts,null,{optional:!0});u!==null&&(l=u.get(gv,[])[Number(i)])}let c={data:l,firstChild:t.firstChild??null};return e&&(c.firstChild=t,qu(c,0,t.nextSibling)),a?t.setAttribute(wu,a):t.removeAttribute(wu),c}function S0(){x0=jk}function I0(t,n,e=!1){return x0(t,n,e)}function M0(t){let n=t._lView;return n[U].type===2?null:(mo(n)&&(n=n[xe]),n)}function Bk(t){return t.textContent?.replace(/\s/gm,"")}function Uk(t){let n=n0(),e=n.createNodeIterator(t,NodeFilter.SHOW_COMMENT,{acceptNode(o){let s=Bk(o);return s==="ngetn"||s==="ngtns"?NodeFilter.FILTER_ACCEPT:NodeFilter.FILTER_REJECT}}),i,r=[];for(;i=e.nextNode();)r.push(i);for(let o of r)o.textContent==="ngetn"?o.replaceWith(n.createTextNode("")):o.remove()}function qu(t,n,e){t.segmentHeads??={},t.segmentHeads[n]=e}function Mg(t,n){return t.segmentHeads?.[n]??null}function T0(t){return t.get(p0,!1,{optional:!0})}function Hk(t,n){let e=t.data,i=e[o0]?.[n]??null;return i===null&&e[Gu]?.[n]&&(i=vv(t,n)),i}function A0(t,n){return t.data[Gu]?.[n]??null}function vv(t,n){let e=A0(t,n)??[],i=0;for(let r of e)i+=r[gl]*(r[dv]??1);return i}function zk(t){if(typeof t.disconnectedNodes>"u"){let n=t.data[uv];t.disconnectedNodes=n?new Set(n):null}return t.disconnectedNodes}function R0(t,n){if(typeof t.disconnectedNodes>"u"){let e=t.data[uv];t.disconnectedNodes=e?new Set(e):null}return!!zk(t)?.has(n)}function Yu(t,n){let e=t[Ht];return e!==null&&!ru()&&!Il(n)&&!R0(e,n.index-xe)}function $k(t,n){let e=n.get(Wu),r=n.get(Ts).get(C0,{}),o=!1,s=t,a=null,l=[];for(;!o&&s;){o=e.has(s);let c=e.hydrating.get(s);if(a===null&&c!=null){a=c.promise;break}l.unshift(s),s=r[s][u0]}return{parentBlockPromise:a,hydrationQueue:l}}function cg(t){return!!t&&t.nodeType===Node.COMMENT_NODE&&t.textContent?.trim()===E0}function qw(t){for(;t&&t.nodeType===Node.TEXT_NODE;)t=t.previousSibling;return t}function k0(t){for(let i of t.body.childNodes)if(cg(i))return;let n=qw(t.body.previousSibling);if(cg(n))return;let e=qw(t.head.lastChild);if(!cg(e))throw new S(-507,!1)}function O0(t,n){let e=t.contentQueries;if(e!==null){let i=X(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],s=e[r+1];if(s!==-1){let a=t.data[s];ll(o),a.contentQueries(2,n[s],s)}}}finally{X(i)}}}function Tg(t,n,e){ll(0);let i=X(null);try{n(t,e)}finally{X(i)}}function _v(t,n,e){if(Pp(n)){let i=X(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=t.data[s];if(a.contentQueries){let l=e[s];a.contentQueries(1,l,s)}}}finally{X(i)}}}var Fn=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(Fn||{});var pu;function Gk(){if(pu===void 0&&(pu=null,fn.trustedTypes))try{pu=fn.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return pu}function Zu(t){return Gk()?.createHTML(t)||t}var gu;function Wk(){if(gu===void 0&&(gu=null,fn.trustedTypes))try{gu=fn.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return gu}function Yw(t){return Wk()?.createScriptURL(t)||t}var Ti=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Gd})`}},Ag=class extends Ti{getTypeName(){return"HTML"}},Rg=class extends Ti{getTypeName(){return"Style"}},kg=class extends Ti{getTypeName(){return"Script"}},Og=class extends Ti{getTypeName(){return"URL"}},Ng=class extends Ti{getTypeName(){return"ResourceURL"}};function Ln(t){return t instanceof Ti?t.changingThisBreaksApplicationSecurity:t}function Ri(t,n){let e=N0(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${Gd})`)}return e===n}function N0(t){return t instanceof Ti&&t.getTypeName()||null}function yv(t){return new Ag(t)}function bv(t){return new Rg(t)}function Dv(t){return new kg(t)}function wv(t){return new Og(t)}function Cv(t){return new Ng(t)}function qk(t){let n=new Fg(t);return Yk()?new Pg(n):n}var Pg=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(Zu(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},Fg=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=Zu(n),e}};function Yk(){try{return!!new window.DOMParser().parseFromString(Zu(""),"text/html")}catch{return!1}}var Zk=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Ml(t){return t=String(t),t.match(Zk)?t:"unsafe:"+t}function ki(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function Tl(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var P0=ki("area,br,col,hr,img,wbr"),F0=ki("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),L0=ki("rp,rt"),Kk=Tl(L0,F0),Qk=Tl(F0,ki("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),Xk=Tl(L0,ki("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),Zw=Tl(P0,Qk,Xk,Kk),V0=ki("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),Jk=ki("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),eO=ki("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),tO=Tl(V0,Jk,eO),nO=ki("script,style,template"),Lg=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=oO(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=rO(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=Kw(n).toLowerCase();if(!Zw.hasOwnProperty(e))return this.sanitizedSomething=!0,!nO.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!tO.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let l=o.value;V0[a]&&(l=Ml(l)),this.buf.push(" ",s,'="',Qw(l),'"')}return this.buf.push(">"),!0}endElement(n){let e=Kw(n).toLowerCase();Zw.hasOwnProperty(e)&&!P0.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(Qw(n))}};function iO(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function rO(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw j0(n);return n}function oO(t){let n=t.firstChild;if(n&&iO(t,n))throw j0(n);return n}function Kw(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function j0(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var sO=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,aO=/([^\#-~ |!])/g;function Qw(t){return t.replace(/&/g,"&amp;").replace(sO,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(aO,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var vu;function Ev(t,n){let e=null;try{vu=vu||qk(t);let i=n?String(n):"";e=vu.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=vu.getInertBodyElement(i)}while(i!==o);let a=new Lg().sanitizeChildren(Xw(e)||e);return Zu(a)}finally{if(e){let i=Xw(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function Xw(t){return"content"in t&&lO(t)?t.content:null}function lO(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var cO=/^>|^->|<!--|-->|--!>|<!-$/g,dO=/(<|>)/g,uO="\u200B$1\u200B";function fO(t){return t.replace(cO,n=>n.replace(dO,uO))}function B0(t,n){return t.createText(n)}function hO(t,n,e){t.setValue(n,e)}function U0(t,n){return t.createComment(fO(n))}function xv(t,n,e){return t.createElement(n,e)}function ku(t,n,e,i,r){t.insertBefore(n,e,i,r)}function H0(t,n,e){t.appendChild(n,e)}function Jw(t,n,e,i,r){i!==null?ku(t,n,e,i,r):H0(t,n,e)}function Sv(t,n,e,i){t.removeChild(null,n,e,i)}function z0(t){t.textContent=""}function mO(t,n,e){t.setAttribute(n,"style",e)}function pO(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function $0(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&mk(t,n,i),r!==null&&pO(t,n,r),o!==null&&mO(t,n,o)}var _t=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(_t||{});function G0(t){let n=q0();return n?n.sanitize(_t.URL,t)||"":Ri(t,"URL")?Ln(t):Ml(ds(t))}function W0(t){let n=q0();if(n)return Yw(n.sanitize(_t.RESOURCE_URL,t)||"");if(Ri(t,"ResourceURL"))return Yw(Ln(t));throw new S(904,!1)}var gO={embed:{src:!0},frame:{src:!0},iframe:{src:!0},media:{src:!0},base:{href:!0},link:{href:!0},object:{data:!0,codebase:!0}};function vO(t,n){return gO[t.toLowerCase()]?.[n.toLowerCase()]===!0?W0:G0}function Iv(t,n,e){return vO(n,e)(t)}function q0(){let t=ie();return t&&t[Tn].sanitizer}function Y0(t){return t.ownerDocument.body}function Z0(t){return t instanceof Function?t():t}function _O(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var K0="ng-template";function yO(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&_O(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(Mv(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function Mv(t){return t.type===4&&t.value!==K0}function bO(t,n,e){let i=t.type===4&&!e?K0:t.value;return n===i}function DO(t,n,e){let i=4,r=t.attrs,o=r!==null?EO(r):0,s=!1;for(let a=0;a<n.length;a++){let l=n[a];if(typeof l=="number"){if(!s&&!On(i)&&!On(l))return!1;if(s&&On(l))continue;s=!1,i=l|i&1;continue}if(!s)if(i&4){if(i=2|i&1,l!==""&&!bO(t,l,e)||l===""&&n.length===1){if(On(i))return!1;s=!0}}else if(i&8){if(r===null||!yO(t,r,l,e)){if(On(i))return!1;s=!0}}else{let c=n[++a],u=wO(l,r,Mv(t),e);if(u===-1){if(On(i))return!1;s=!0;continue}if(c!==""){let f;if(u>o?f="":f=r[u+1].toLowerCase(),i&2&&c!==f){if(On(i))return!1;s=!0}}}}return On(i)||s}function On(t){return(t&1)===0}function wO(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let s=n[r];if(s===t)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return xO(n,t)}function Q0(t,n,e=!1){for(let i=0;i<n.length;i++)if(DO(t,n[i],e))return!0;return!1}function CO(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function EO(t){for(let n=0;n<t.length;n++){let e=t[n];if(LC(e))return n}return t.length}function xO(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function SO(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function eC(t,n){return t?":not("+n.trim()+")":n}function IO(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let s=t[e];if(typeof s=="string")if(i&2){let a=t[++e];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!On(s)&&(n+=eC(o,r),r=""),i=s,o=o||!On(i);e++}return r!==""&&(n+=eC(o,r)),n}function MO(t){return t.map(IO).join(",")}function TO(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!On(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var $t={};function Tv(t,n,e,i,r,o,s,a,l,c,u){let f=xe+i,h=f+r,m=AO(f,h),p=typeof c=="function"?c():c;return m[U]={type:t,blueprint:m,template:e,queries:null,viewQuery:a,declTNode:n,data:m.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:h,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:l,consts:p,incompleteFirstPass:!1,ssrId:u}}function AO(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:$t);return e}function RO(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=Tv(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function Av(t,n,e,i,r,o,s,a,l,c,u){let f=n.blueprint.slice();return f[Bt]=r,f[se]=i|4|128|8|64|1024,(c!==null||t&&t[se]&2048)&&(f[se]|=2048),Bp(f),f[st]=f[co]=t,f[at]=e,f[Tn]=s||t&&t[Tn],f[Ee]=a||t&&t[Ee],f[Xt]=l||t&&t[Xt]||null,f[Ut]=o,f[Ii]=Tk(),f[Ht]=u,f[Np]=c,f[St]=n.type==2?t[St]:f,f}function kO(t,n,e){let i=pn(n,t),r=RO(e),o=t[Tn].rendererFactory,s=Rv(t,Av(t,r,null,X0(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=s}function X0(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function J0(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function Rv(t,n){return t[ms]?t[Op][hn]=n:t[ms]=n,t[Op]=n,n}function D(t=1){eE(Ue(),ie(),ri()+t,!1)}function eE(t,n,e,i){if(!i)if((n[se]&3)===3){let o=t.preOrderCheckHooks;o!==null&&yu(n,o,e)}else{let o=t.preOrderHooks;o!==null&&bu(n,o,0,e)}pr(e)}var Ku=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(Ku||{});function Vg(t,n,e,i){let r=X(null);try{let[o,s,a]=t.inputs[e],l=null;(s&Ku.SignalBased)!==0&&(l=n[o][it]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):a!==null&&(i=a.call(n,i)),t.setInput!==null?t.setInput(n,l,i,e,o):RC(n,l,o,i)}finally{X(r)}}var li=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(li||{}),OO;function kv(t,n){return OO(t,n)}var Y6=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var jg=new WeakMap,fl=new WeakSet;function NO(t,n){let e=jg.get(t);if(!e||e.length===0)return;let i=n.parentNode,r=n.previousSibling;for(let o=e.length-1;o>=0;o--){let s=e[o],a=s.parentNode;s===n?(e.splice(o,1),fl.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&s===r||a&&i&&a!==i)&&(e.splice(o,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function PO(t,n){let e=jg.get(t);e?e.includes(n)||e.push(n):jg.set(t,[n])}var yo=new Set,Qu=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(Qu||{}),Vn=new g(""),tC=new Set;function yn(t){tC.has(t)||(tC.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var Xu=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=b({token:t,providedIn:"root",factory:()=>new t})}return t})(),Ov=[0,1,2,3],Nv=(()=>{class t{ngZone=d(G);scheduler=d(Sn);errorHandler=d(jt,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){d(Vn,{optional:!0})}execute(){let e=this.sequences.size>0;e&&Se(we.AfterRenderHooksStart),this.executing=!0;for(let i of Ov)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&Se(we.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[fo]??=[]).push(e),po(i),i[se]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(Qu.AFTER_NEXT_RENDER,e):e()}static \u0275prov=b({token:t,providedIn:"root",factory:()=>new t})}return t})(),vl=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,s=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[fo];n&&(this.view[fo]=n.filter(e=>e!==this))}};function Ke(t,n){let e=n?.injector??d(W);return yn("NgAfterNextRender"),LO(t,e,n,!0)}function FO(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function LO(t,n,e,i){let r=n.get(Xu);r.impl??=n.get(Nv);let o=n.get(Vn,null,{optional:!0}),s=e?.manualCleanup!==!0?n.get(zt):null,a=n.get(_s,null,{optional:!0}),l=new vl(r.impl,FO(t),a?.view,i,s,o?.snapshot(null));return r.impl.register(l),l}var tE=new g("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:d(Ne)})});function nE(t,n,e){let i=t.get(tE);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function VO(t,n){let e=t.get(tE);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)e.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function jO(t,n){for(let[e,i]of n)nE(t,i.animateFns)}function nC(t,n,e,i){let r=t?.[fr]?.enter;n!==null&&r&&r.has(e.index)&&jO(i,r)}function bs(t,n,e,i,r,o,s,a){if(r!=null){let l,c=!1;en(r)?l=r:mn(r)&&(c=!0,r=r[Bt]);let u=tn(r);t===0&&i!==null?(nC(a,i,o,e),s==null?H0(n,i,u):ku(n,i,u,s||null,!0)):t===1&&i!==null?(nC(a,i,o,e),ku(n,i,u,s||null,!0),NO(o,u)):t===2?(a?.[fr]?.leave?.has(o.index)&&PO(o,u),fl.delete(u),iC(a,o,e,f=>{if(fl.has(u)){fl.delete(u);return}Sv(n,u,c,f)})):t===3&&(fl.delete(u),iC(a,o,e,()=>{n.destroyNode(u)})),l!=null&&KO(n,t,e,l,o,i,s)}}function BO(t,n){iE(t,n),n[Bt]=null,n[Ut]=null}function UO(t,n,e,i,r,o){i[Bt]=r,i[Ut]=n,ef(t,i,e,1,r,o)}function iE(t,n){n[Tn].changeDetectionScheduler?.notify(9),ef(t,n,n[Ee],2,null,null)}function HO(t){let n=t[ms];if(!n)return dg(t[U],t);for(;n;){let e=null;if(mn(n))e=n[ms];else{let i=n[qe];i&&(e=i)}if(!e){for(;n&&!n[hn]&&n!==t;)mn(n)&&dg(n[U],n),n=n[st];n===null&&(n=t),mn(n)&&dg(n[U],n),e=n&&n[hn]}n=e}}function Pv(t,n){let e=t[ho],i=e.indexOf(n);e.splice(i,1)}function Ju(t,n){if(hr(n))return;let e=n[Ee];e.destroyNode&&ef(t,n,e,3,null,null),HO(n)}function dg(t,n){if(hr(n))return;let e=X(null);try{n[se]&=-129,n[se]|=256,n[Jt]&&tr(n[Jt]),GO(t,n),$O(t,n),n[U].type===1&&n[Ee].destroy();let i=n[ur];if(i!==null&&en(n[st])){i!==n[st]&&Pv(i,n);let r=n[ti];r!==null&&r.detachView(t)}Eg(n)}finally{X(e)}}function iC(t,n,e,i){let r=t?.[fr];if(r==null||r.leave==null||!r.leave.has(n.index))return i(!1);t&&yo.add(t[Ii]),nE(e,()=>{if(r.leave&&r.leave.has(n.index)){let s=r.leave.get(n.index),a=[];if(s){for(let l=0;l<s.animateFns.length;l++){let c=s.animateFns[l],{promise:u}=c();a.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),zO(t,i)}else t&&yo.delete(t[Ii]),i(!1)},r)}function zO(t,n){let e=t[fr]?.running;if(e){e.then(()=>{t[fr].running=void 0,yo.delete(t[Ii]),n(!0)});return}n(!1)}function $O(t,n){let e=t.cleanup,i=n[hs];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[e[s+1]];e[s].call(a)}i!==null&&(n[hs]=null);let r=n[Si];if(r!==null){n[Si]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[cr];if(o!==null){n[cr]=null;for(let s of o)s.destroy()}}function GO(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof _o)){let o=e[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],l=o[s+1];Se(we.LifecycleHookStart,a,l);try{l.call(a)}finally{Se(we.LifecycleHookEnd,a,l)}}else{Se(we.LifecycleHookStart,r,o);try{o.call(r)}finally{Se(we.LifecycleHookEnd,r,o)}}}}}function rE(t,n,e){return WO(t,n.parent,e)}function WO(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[Bt];if(ii(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===Fn.None||r===Fn.Emulated)return null}return pn(i,e)}function oE(t,n,e){return YO(t,n,e)}function qO(t,n,e){return t.type&40?pn(t,e):null}var YO=qO,rC;function Fv(t,n,e,i){let r=rE(t,i,n),o=n[Ee],s=i.parent||n[Ut],a=oE(s,i,n);if(r!=null)if(Array.isArray(e))for(let l=0;l<e.length;l++)Jw(o,r,e[l],a,!1);else Jw(o,r,e,a,!1);rC!==void 0&&rC(o,i,n,e,r)}function hl(t,n){if(n!==null){let e=n.type;if(e&3)return pn(n,t);if(e&4)return Bg(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return hl(t,i);{let r=t[n.index];return en(r)?Bg(-1,r):tn(r)}}else{if(e&128)return hl(t,n.next);if(e&32)return kv(n,t)()||tn(t[n.index]);{let i=sE(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=dr(t[St]);return hl(r,i)}else return hl(t,n.next)}}}return null}function sE(t,n){if(n!==null){let i=t[St][Ut],r=n.projection;return i.projection[r]}return null}function Bg(t,n){let e=qe+t+1;if(e<n.length){let i=n[e],r=i[U].firstChild;if(r!==null)return hl(i,r)}return n[ni]}function Lv(t,n,e,i,r,o,s){for(;e!=null;){let a=i[Xt];if(e.type===128){e=e.next;continue}let l=i[e.index],c=e.type;if(s&&n===0&&(l&&xs(tn(l),i),e.flags|=2),!Il(e))if(c&8)Lv(t,n,e.child,i,r,o,!1),bs(n,t,a,r,l,e,o,i);else if(c&32){let u=kv(e,i),f;for(;f=u();)bs(n,t,a,r,f,e,o,i);bs(n,t,a,r,l,e,o,i)}else c&16?aE(t,n,i,e,r,o):bs(n,t,a,r,l,e,o,i);e=s?e.projectionNext:e.next}}function ef(t,n,e,i,r,o){Lv(e,i,t.firstChild,n,r,o,!1)}function ZO(t,n,e){let i=n[Ee],r=rE(t,e,n),o=e.parent||n[Ut],s=oE(o,e,n);aE(i,0,n,e,r,s)}function aE(t,n,e,i,r,o){let s=e[St],l=s[Ut].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let u=l[c];bs(n,t,e[Xt],r,u,i,o,e)}else{let c=l,u=s[st];Ru(i)&&(c.flags|=128),Lv(t,n,c,u,r,o,!0)}}function KO(t,n,e,i,r,o,s){let a=i[ni],l=tn(i);a!==l&&bs(n,t,e,o,a,r,s);for(let c=qe;c<i.length;c++){let u=i[c];ef(u[U],u,t,n,o,a)}}function QO(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:li.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=li.Important),t.setStyle(e,i,r,o))}}function lE(t,n,e,i,r){let o=ri(),s=i&2;try{pr(-1),s&&n.length>xe&&eE(t,n,xe,!1);let a=s?we.TemplateUpdateStart:we.TemplateCreateStart;Se(a,r,e),e(i,r)}finally{pr(o);let a=s?we.TemplateUpdateEnd:we.TemplateCreateEnd;Se(a,r,e)}}function tf(t,n,e){iN(t,n,e),(e.flags&64)===64&&rN(t,n,e)}function Al(t,n,e=pn){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?e(n,t):t[s];t[r++]=a}}}function XO(t,n,e,i){let o=i.get(fv,f0)||e===Fn.ShadowDom||e===Fn.ExperimentalIsolatedShadowDom,s=t.selectRootElement(n,o);if(s.tagName.toLowerCase()==="script")throw new S(905,!1);return JO(s),s}function JO(t){cE(t)}var cE=()=>null;function eN(t){KC(t)?z0(t):Uk(t)}function dE(){cE=eN}function tN(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function uE(t,n,e,i,r,o){let s=n[U];if(Hv(t,s,n,e,i)){ii(t)&&nN(n,t.index);return}t.type&3&&(e=tN(e)),fE(t,n,e,i,r,o)}function fE(t,n,e,i,r,o){if(t.type&3){let s=pn(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(s,e,i)}else t.type&12}function nN(t,n){let e=gn(n,t);e[se]&16||(e[se]|=64)}function iN(t,n,e){let i=e.directiveStart,r=e.directiveEnd;ii(e)&&kO(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||Au(e,n);let o=e.initialInputs;for(let s=i;s<r;s++){let a=t.data[s],l=pl(n,t,s,e);if(xs(l,n),o!==null&&lN(n,s-i,l,a,e,o),Rn(a)){let c=gn(e.index,n);c[at]=pl(n,t,s,e)}}}function rN(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,s=Sw();try{pr(o);for(let a=i;a<r;a++){let l=t.data[a],c=n[a];su(a),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&oN(l,c)}}finally{pr(-1),su(s)}}function oN(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function Vv(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];Q0(n,o.selectors,!1)&&(i??=[],Rn(o)?i.unshift(o):i.push(o))}return i}function sN(t,n,e,i,r,o){let s=pn(t,n);aN(n[Ee],s,o,t.value,e,i,r)}function aN(t,n,e,i,r,o,s){if(o==null)t.removeAttribute(n,r,e);else{let a=s==null?ds(o):s(o,i||"",r);t.setAttribute(n,r,a,e)}}function lN(t,n,e,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let l=s[a],c=s[a+1];Vg(i,e,l,c)}}function jv(t,n,e,i,r){let o=xe+e,s=n[U],a=r(s,n,t,i,e);n[o]=a,vs(t,!0);let l=t.type===2;return l?($0(n[Ee],a,t),(vw()===0||ps(t))&&xs(a,n),_w()):xs(a,n),uu()&&(!l||!Il(t))&&Fv(s,n,a,t),t}function Bv(t){let n=t;return Zp()?Kp():(n=n.parent,vs(n,!1)),n}function Uv(t,n){let e=t[Xt];if(!e)return;let i;try{i=e.get(rn,null)}catch{i=null}i?.(n)}function Hv(t,n,e,i,r){let o=t.inputs?.[i],s=t.hostDirectiveInputs?.[i],a=!1;if(s)for(let l=0;l<s.length;l+=2){let c=s[l],u=s[l+1],f=n.data[c];Vg(f,e[c],u,r),a=!0}if(o)for(let l of o){let c=e[l],u=n.data[l];Vg(u,c,i,r),a=!0}return a}function cN(t,n){let e=gn(n,t),i=e[U];dN(i,e);let r=e[Bt];r!==null&&e[Ht]===null&&(e[Ht]=I0(r,e[Xt])),Se(we.ComponentStart);try{zv(i,e,e[at])}finally{Se(we.ComponentEnd,e[at])}}function dN(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function zv(t,n,e){lu(n);try{let i=t.viewQuery;i!==null&&Tg(1,i,e);let r=t.template;r!==null&&lE(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[ti]?.finishViewCreation(t),t.staticContentQueries&&O0(t,n),t.staticViewQueries&&Tg(2,t.viewQuery,e);let o=t.components;o!==null&&uN(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[se]&=-5,cu()}}function uN(t,n){for(let e=0;e<n.length;e++)cN(t,n[e])}function Rs(t,n,e,i){let r=X(null);try{let o=n.tView,a=t[se]&4096?4096:16,l=Av(t,o,e,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=t[n.index];l[ur]=c;let u=t[ti];return u!==null&&(l[ti]=u.createEmbeddedView(o)),zv(o,l,e),l}finally{X(r)}}function bo(t,n){return!n||n.firstChild===null||Ru(t)}function _l(t,n,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];o!==null&&i.push(tn(o)),en(o)&&hE(o,i);let s=e.type;if(s&8)_l(t,n,e.child,i);else if(s&32){let a=kv(e,n),l;for(;l=a();)i.push(l)}else if(s&16){let a=sE(n,e);if(Array.isArray(a))i.push(...a);else{let l=dr(n[St]);_l(l[U],l,a,i,!0)}}e=r?e.projectionNext:e.next}return i}function hE(t,n){for(let e=qe;e<t.length;e++){let i=t[e],r=i[U].firstChild;r!==null&&_l(i[U],i,r,n)}t[ni]!==t[Bt]&&n.push(t[ni])}function mE(t){if(t[fo]!==null){for(let n of t[fo])n.impl.addSequence(n);t[fo].length=0}}var pE=[];function fN(t){return t[Jt]??hN(t)}function hN(t){let n=pE.pop()??Object.create(pN);return n.lView=t,n}function mN(t){t.lView[Jt]!==t&&(t.lView=null,pE.push(t))}var pN=te(y({},Xi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{po(t.lView)},consumerOnSignalRead(){this.lView[Jt]=this}});function gN(t){let n=t[Jt]??Object.create(vN);return n.lView=t,n}var vN=te(y({},Xi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=dr(t.lView);for(;n&&!gE(n[U]);)n=dr(n);n&&Up(n)},consumerOnSignalRead(){this.lView[Jt]=this}});function gE(t){return t.type!==2}function vE(t){if(t[cr]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[cr])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[se]&8192)}}var _N=100;function _E(t,n=0){let i=t[Tn].rendererFactory,r=!1;r||i.begin?.();try{yN(t,n)}finally{r||i.end?.()}}function yN(t,n){let e=Qp();try{Xa(!0),Ug(t,n);let i=0;for(;al(t);){if(i===_N)throw new S(103,!1);i++,Ug(t,1)}}finally{Xa(e)}}function bN(t,n,e,i){if(hr(n))return;let r=n[se],o=!1,s=!1;lu(n);let a=!0,l=null,c=null;o||(gE(t)?(c=fN(n),l=Ci(c)):id()===null?(a=!1,c=gN(n),l=Ci(c)):n[Jt]&&(tr(n[Jt]),n[Jt]=null));try{Bp(n),Cw(t.bindingStartIndex),e!==null&&lE(t,n,e,2,i);let u=(r&3)===3;if(!o)if(u){let m=t.preOrderCheckHooks;m!==null&&yu(n,m,null)}else{let m=t.preOrderHooks;m!==null&&bu(n,m,0,null),ag(n,0)}if(s||DN(n),vE(n),yE(n,0),t.contentQueries!==null&&O0(t,n),!o)if(u){let m=t.contentCheckHooks;m!==null&&yu(n,m)}else{let m=t.contentHooks;m!==null&&bu(n,m,1),ag(n,1)}CN(t,n);let f=t.components;f!==null&&DE(n,f,0);let h=t.viewQuery;if(h!==null&&Tg(2,h,i),!o)if(u){let m=t.viewCheckHooks;m!==null&&yu(n,m)}else{let m=t.viewHooks;m!==null&&bu(n,m,2),ag(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[eu]){for(let m of n[eu])m();n[eu]=null}o||(mE(n),n[se]&=-73)}catch(u){throw o||po(n),u}finally{c!==null&&(er(c,l),a&&mN(c)),cu()}}function yE(t,n){for(let e=JC(t);e!==null;e=e0(e))for(let i=qe;i<e.length;i++){let r=e[i];bE(r,n)}}function DN(t){for(let n=JC(t);n!==null;n=e0(n)){if(!(n[se]&2))continue;let e=n[ho];for(let i=0;i<e.length;i++){let r=e[i];Up(r)}}}function wN(t,n,e){Se(we.ComponentStart);let i=gn(n,t);try{bE(i,e)}finally{Se(we.ComponentEnd,i[at])}}function bE(t,n){tu(t)&&Ug(t,n)}function Ug(t,n){let i=t[U],r=t[se],o=t[Jt],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&Jo(o)),s||=!1,o&&(o.dirty=!1),t[se]&=-9217,s)bN(i,t,i.template,t[at]);else if(r&8192){let a=X(null);try{vE(t),yE(t,1);let l=i.components;l!==null&&DE(t,l,1),mE(t)}finally{X(a)}}}function DE(t,n,e){for(let i=0;i<n.length;i++)wN(t,n[i],e)}function CN(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)pr(~r);else{let o=r,s=e[++i],a=e[++i];xw(s,o);let l=n[o];Se(we.HostBindingsUpdateStart,l);try{a(2,l)}finally{Se(we.HostBindingsUpdateEnd,l)}}}}finally{pr(-1)}}function nf(t,n){let e=Qp()?64:1088;for(t[Tn].changeDetectionScheduler?.notify(n);t;){t[se]|=e;let i=dr(t);if(mo(t)&&!i)return t;t=i}return null}function wE(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function CE(t,n){let e=qe+n;if(e<t.length)return t[e]}function ks(t,n,e,i=!0){let r=n[U];if(EN(r,n,t,e),i){let s=Bg(e,t),a=n[Ee],l=a.parentNode(t[ni]);l!==null&&UO(r,t[Ut],a,n,l,s)}let o=n[Ht];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function $v(t,n){let e=yl(t,n);return e!==void 0&&Ju(e[U],e),e}function yl(t,n){if(t.length<=qe)return;let e=qe+n,i=t[e];if(i){let r=i[ur];r!==null&&r!==t&&Pv(r,i),n>0&&(t[e-1][hn]=i[hn]);let o=rl(t,qe+n);BO(i[U],i);let s=o[ti];s!==null&&s.detachView(o[U]),i[st]=null,i[hn]=null,i[se]&=-129}return i}function EN(t,n,e,i){let r=qe+i,o=e.length;i>0&&(e[r-1][hn]=n),i<o-qe?(n[hn]=e[r],Mp(e,qe+i,n)):(e.push(n),n[hn]=null),n[st]=e;let s=n[ur];s!==null&&e!==s&&EE(s,n);let a=n[ti];a!==null&&a.insertView(t),nu(n),n[se]|=128}function EE(t,n){let e=t[ho],i=n[st];if(mn(i))t[se]|=2;else{let r=i[st][St];n[St]!==r&&(t[se]|=2)}e===null?t[ho]=[n]:e.push(n)}var gr=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[U];return _l(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[at]}set context(n){this._lView[at]=n}get destroyed(){return hr(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[st];if(en(n)){let e=n[sl],i=e?e.indexOf(this):-1;i>-1&&(yl(n,i),rl(e,i))}this._attachedToViewContainer=!1}Ju(this._lView[U],this._lView)}onDestroy(n){Hp(this._lView,n)}markForCheck(){nf(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[se]&=-129}reattach(){nu(this._lView),this._lView[se]|=128}detectChanges(){this._lView[se]|=1024,_E(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new S(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=mo(this._lView),e=this._lView[ur];e!==null&&!n&&Pv(e,this._lView),iE(this._lView[U],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new S(902,!1);this._appRef=n;let e=mo(this._lView),i=this._lView[ur];i!==null&&!e&&EE(i,this._lView),nu(this._lView)}};var je=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=xN;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=Rs(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new gr(o)}}return t})();function xN(){return rf(vt(),ie())}function rf(t,n){return t.type&4?new je(n,t,Ms(t,n)):null}function Os(t,n,e,i,r){let o=t.data[n];if(o===null)o=SN(t,n,e,i,r),Ew()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let s=bw();o.injectorIndex=s===null?-1:s.injectorIndex}return vs(o,!0),o}function SN(t,n,e,i,r){let o=Yp(),s=Zp(),a=s?o:o&&o.parent,l=t.data[n]=MN(t,a,e,n,i,r);return IN(t,l,o,s),l}function IN(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function MN(t,n,e,i,r,o){let s=n?n.injectorIndex:-1,a=0;return ru()&&(a|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:du(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var TN=new RegExp(`^(\\d+)*(${r0}|${i0})*(.*)`);function AN(t){let n=t.match(TN),[e,i,r,o]=n,s=i?parseInt(i,10):r,a=[];for(let[l,c,u]of o.matchAll(/(f|n)(\d*)/g)){let f=parseInt(u,10)||1;a.push(c,f)}return[s,...a]}function RN(t){return!t.prev&&t.parent?.type===8}function ug(t){return t.index-xe}function kN(t,n){let e=t.i18nNodes;if(e)return e.get(n)}function of(t,n,e,i){let r=ug(i),o=kN(t,r);if(o===void 0){let s=t.data[l0];if(s?.[r])o=NN(s[r],e);else if(n.firstChild===i)o=t.firstChild;else{let a=i.prev===null,l=i.prev??i.parent;if(RN(i)){let c=ug(i.parent);o=Mg(t,c)}else{let c=pn(l,e);if(a)o=c.firstChild;else{let u=ug(l),f=Mg(t,u);if(l.type===2&&f){let m=vv(t,u)+1;o=sf(m,f)}else o=c.nextSibling}}}}return o}function sf(t,n){let e=n;for(let i=0;i<t;i++)e=e.nextSibling;return e}function ON(t,n){let e=t;for(let i=0;i<n.length;i+=2){let r=n[i],o=n[i+1];for(let s=0;s<o;s++)switch(r){case Ok:e=e.firstChild;break;case Nk:e=e.nextSibling;break}}return e}function NN(t,n){let[e,...i]=AN(t),r;if(e===i0)r=n[St][Bt];else if(e===r0)r=Y0(n[St][Bt]);else{let o=Number(e);r=tn(n[o+xe])}return ON(r,i)}var PN=!1;function xE(t){PN=t}function FN(t){let n=t[Ht];if(n){let{i18nNodes:e,dehydratedIcuData:i}=n;if(e&&i){let r=t[Ee];for(let o of i.values())LN(r,e,o)}n.i18nNodes=void 0,n.dehydratedIcuData=void 0}}function LN(t,n,e){for(let i of e.node.cases[e.case]){let r=n.get(i.index-xe);r&&Sv(t,r,!1)}}function af(t){let n=t[An]??[],i=t[st][Ee],r=[];for(let o of n)o.data[c0]!==void 0?r.push(o):SE(o,i);t[An]=r}function VN(t){let{lContainer:n}=t,e=n[An];if(e===null)return;let r=n[st][Ee];for(let o of e)SE(o,r)}function SE(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[gl];for(;e<r;){let o=i.nextSibling;Sv(n,i,!1),i=o,e++}}}function lf(t){af(t);let n=t[Bt];mn(n)&&Ou(n);for(let e=qe;e<t.length;e++)Ou(t[e])}function Ou(t){FN(t);let n=t[U];for(let e=xe;e<n.bindingStartIndex;e++)if(en(t[e])){let i=t[e];lf(i)}else mn(t[e])&&Ou(t[e])}function Gv(t){let n=t._views;for(let e of n){let i=M0(e);i!==null&&i[Bt]!==null&&(mn(i)?Ou(i):lf(i))}}function jN(t,n,e,i){t!==null&&(e.cleanup(n),lf(t.lContainer),Gv(i))}function BN(t,n){let e=[];for(let i of n)for(let r=0;r<(i[dv]??1);r++){let o={data:i,firstChild:null};i[gl]>0&&(o.firstChild=t,t=sf(i[gl],t)),e.push(o)}return[t,e]}var IE=()=>null,ME=()=>null;function TE(){IE=UN,ME=HN}function UN(t,n){return RE(t,n)?t[An].shift():(af(t),null)}function bl(t,n){return IE(t,n)}function HN(t,n,e){if(n.tView.ssrId===null)return null;let i=bl(t,n.tView.ssrId);return e[U].firstUpdatePass&&i===null&&zN(e,n),i}function AE(t,n,e){return ME(t,n,e)}function zN(t,n){let e=n;for(;e;){if(oC(t,e))return;if((e.flags&256)===256)break;e=e.prev}for(e=n.next;e&&(e.flags&512)===512;){if(oC(t,e))return;e=e.next}}function RE(t,n){let e=t[An];return!n||e===null||e.length===0?!1:e[0].data[a0]===n}function oC(t,n){let e=n.tView?.ssrId;if(e==null)return!1;let i=t[n.index];return en(i)&&RE(i,e)?(af(i),!0):!1}var kE=class{},cf=class{},Hg=class{resolveComponentFactory(n){throw new S(917,!1)}},Rl=class{static NULL=new Hg},ft=class{},Be=(()=>{class t{destroyNode=null;static __NG_ELEMENT_ID__=()=>$N()}return t})();function $N(){let t=ie(),n=vt(),e=gn(n.index,t);return(mn(e)?e:t)[Ee]}var OE=(()=>{class t{static \u0275prov=b({token:t,providedIn:"root",factory:()=>null})}return t})();var Cu={},ws=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,Cu,i);return r!==Cu||e===Cu?r:this.parentInjector.get(n,e,i)}};function Nu(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=Wd(r,a);else if(o==2){let l=a,c=n[++s];i=Wd(i,l+": "+c+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function ne(t,n=0){let e=ie();if(e===null)return K(t,n);let i=vt();return $C(i,e,gt(t),n)}function df(){let t="invalid";throw new Error(t)}function NE(t,n,e,i,r){let o=i===null?null:{"":-1},s=r(t,e);if(s!==null){let a=s,l=null,c=null;for(let u of s)if(u.resolveHostDirectives!==null){[a,l,c]=u.resolveHostDirectives(s);break}qN(t,n,e,a,o,l,c)}o!==null&&i!==null&&GN(e,i,o)}function GN(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new S(-301,!1);i.push(n[r],o)}}function WN(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function qN(t,n,e,i,r,o,s){let a=i.length,l=null;for(let h=0;h<a;h++){let m=i[h];l===null&&Rn(m)&&(l=m,WN(t,e,h)),wg(Au(e,n),t,m.type)}JN(e,t.data.length,a),l?.viewProvidersResolver&&l.viewProvidersResolver(l);for(let h=0;h<a;h++){let m=i[h];m.providersResolver&&m.providersResolver(m)}let c=!1,u=!1,f=J0(t,n,a,null);a>0&&(e.directiveToIndex=new Map);for(let h=0;h<a;h++){let m=i[h];if(e.mergedAttrs=Es(e.mergedAttrs,m.hostAttrs),ZN(t,e,n,f,m),XN(f,m,r),s!==null&&s.has(m)){let[w,E]=s.get(m);e.directiveToIndex.set(m.type,[f,w+e.directiveStart,E+e.directiveStart])}else(o===null||!o.has(m))&&e.directiveToIndex.set(m.type,f);m.contentQueries!==null&&(e.flags|=4),(m.hostBindings!==null||m.hostAttrs!==null||m.hostVars!==0)&&(e.flags|=64);let p=m.type.prototype;!c&&(p.ngOnChanges||p.ngOnInit||p.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),c=!0),!u&&(p.ngOnChanges||p.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),u=!0),f++}YN(t,e,o)}function YN(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))sC(0,n,r,i),sC(1,n,r,i),lC(n,i,!1);else{let o=e.get(r);aC(0,n,o,i),aC(1,n,o,i),lC(n,i,!0)}}}function sC(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;t===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),PE(n,o)}}function aC(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;t===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),PE(n,s)}}function PE(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function lC(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||Mv(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let l=i[a];if(l===0){a+=4;continue}else if(l===5){a+=2;continue}else if(typeof l=="number")break;if(!e&&r.hasOwnProperty(l)){let c=r[l];for(let u of c)if(u===n){s??=[],s.push(l,i[a+1]);break}}else if(e&&o.hasOwnProperty(l)){let c=o[l];for(let u=0;u<c.length;u+=2)if(c[u]===n){s??=[],s.push(c[u+1],i[a+1]);break}}a+=2}t.initialInputs??=[],t.initialInputs.push(s)}function ZN(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=lr(r.type,!0)),s=new _o(o,Rn(r),ne,null);t.blueprint[i]=s,e[i]=s,KN(t,n,i,J0(t,e,r.hostVars,$t),r)}function KN(t,n,e,i,r){let o=r.hostBindings;if(o){let s=t.hostBindingOpCodes;s===null&&(s=t.hostBindingOpCodes=[]);let a=~n.index;QN(s)!=a&&s.push(a),s.push(e,i,o)}}function QN(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function XN(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;Rn(n)&&(e[""]=t)}}function JN(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function Wv(t,n,e,i,r,o,s,a){let l=n[U],c=l.consts,u=nn(c,s),f=Os(l,t,e,i,u);return o&&NE(l,n,f,nn(c,a),r),f.mergedAttrs=Es(f.mergedAttrs,f.attrs),f.attrs!==null&&Nu(f,f.attrs,!1),f.mergedAttrs!==null&&Nu(f,f.mergedAttrs,!0),l.queries!==null&&l.queries.elementStart(l,f),f}function qv(t,n){PC(t,n),Pp(n)&&t.queries.elementEnd(n)}function eP(t,n,e,i,r,o){let s=n.consts,a=nn(s,r),l=Os(n,t,e,i,a);if(l.mergedAttrs=Es(l.mergedAttrs,l.attrs),o!=null){let c=nn(s,o);l.localNames=[];for(let u=0;u<c.length;u+=2)l.localNames.push(c[u],-1)}return l.attrs!==null&&Nu(l,l.attrs,!1),l.mergedAttrs!==null&&Nu(l,l.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,l),l}function Yv(t){return LE(t)?Array.isArray(t)||!(t instanceof Map)&&Symbol.iterator in t:!1}function FE(t,n){if(Array.isArray(t))for(let e=0;e<t.length;e++)n(t[e]);else{let e=t[Symbol.iterator](),i;for(;!(i=e.next()).done;)n(i.value)}}function LE(t){return t!==null&&(typeof t=="function"||typeof t=="object")}function VE(t,n,e){return t[n]=e}function tP(t,n){return t[n]}function vn(t,n,e){if(e===$t)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function nP(t,n,e,i){let r=vn(t,n,e);return vn(t,n+1,i)||r}function Eu(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&Pk(r,o);let s=ii(t)?gn(t.index,n):n;nf(s,5);let a=n[at],l=cC(n,a,e,r),c=i.__ngNextListenerFn__;for(;c;)l=cC(n,a,c,r)&&l,c=c.__ngNextListenerFn__;return l}}function cC(t,n,e,i){let r=X(null);try{return Se(we.OutputStart,n,e),e(i)!==!1}catch(o){return Uv(t,o),!1}finally{Se(we.OutputEnd,n,e),X(r)}}function jE(t,n,e,i,r,o,s,a){let l=ps(t),c=!1,u=null;if(!i&&l&&(u=rP(n,e,o,t.index)),u!==null){let f=u.__ngLastListenerFn__||u;f.__ngNextListenerFn__=s,u.__ngLastListenerFn__=s,c=!0}else{let f=pn(t,e),h=i?i(f):f;Fk(e,h,o,a),i||(a.__ngNativeEl__=f);let m=r.listen(h,o,a);if(!iP(o)){let p=i?w=>i(tn(w[t.index])):t.index;BE(p,n,e,o,a,m,!1)}}return c}function iP(t){return t.startsWith("animation")||t.startsWith("transition")}function rP(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===e&&r[o+1]===i){let a=n[hs],l=r[o+2];return a&&a.length>l?a[l]:null}typeof s=="string"&&(o+=2)}return null}function BE(t,n,e,i,r,o,s){let a=n.firstCreatePass?$p(n):null,l=zp(e),c=l.length;l.push(r,o),a&&a.push(i,t,c,(c+1)*(s?-1:1))}function dC(t,n,e,i,r,o){let s=n[e],a=n[U],c=a.data[e].outputs[i],f=s[c].subscribe(o);BE(t.index,a,n,r,o,f,!0)}var zg=Symbol("BINDING");function UE(t){return t.debugInfo?.className||t.type.name||null}var Pu=class extends Rl{ngModule;constructor(n){super(),this.ngModule=n}resolveComponentFactory(n){let e=In(n);return new vr(e,this.ngModule)}};function oP(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&Ku.SignalBased)!==0};return r&&(o.transform=r),o})}function sP(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function aP(t,n,e){let i=n instanceof Ne?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new ws(e,i):e}function lP(t){let n=t.get(ft,null);if(n===null)throw new S(407,!1);let e=t.get(OE,null),i=t.get(Sn,null),r=t.get(Vn,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function cP(t,n){let e=HE(t);return xv(n,e,e==="svg"?Fp:e==="math"?uw:null)}function HE(t){return(t.selectors[0][0]||"div").toLowerCase()}var vr=class extends cf{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=oP(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=sP(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){super(),this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=MO(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,s){Se(we.DynamicComponentStart);let a=X(null);try{let l=this.componentDef,c=aP(l,r||this.ngModule,n),u=lP(c),f=u.tracingService;return f&&f.componentCreate?f.componentCreate(UE(l),()=>this.createComponentRef(u,c,e,i,o,s)):this.createComponentRef(u,c,e,i,o,s)}finally{X(a)}}createComponentRef(n,e,i,r,o,s){let a=this.componentDef,l=dP(r,a,s,o),c=n.rendererFactory.createRenderer(null,a),u=r?XO(c,r,a.encapsulation,e):cP(a,c),f=s?.some(uC)||o?.some(p=>typeof p!="function"&&p.bindings.some(uC)),h=Av(null,l,null,512|X0(a),null,null,n,c,e,null,I0(u,e,!0));h[xe]=u,lu(h);let m=null;try{let p=Wv(xe,h,2,"#host",()=>l.directiveRegistry,!0,0);$0(c,u,p),xs(u,h),tf(l,h,p),_v(l,p,h),qv(l,p),i!==void 0&&fP(p,this.ngContentSelectors,i),m=gn(p.index,h),h[at]=m[at],zv(l,h,null)}catch(p){throw m!==null&&Eg(m),Eg(h),p}finally{Se(we.DynamicComponentEnd),cu()}return new Fu(this.componentType,h,!!f)}};function dP(t,n,e,i){let r=t?["ng-version","21.2.15"]:TO(n.selectors[0]),o=null,s=null,a=0;if(e)for(let u of e)a+=u[zg].requiredVars,u.create&&(u.targetIdx=0,(o??=[]).push(u)),u.update&&(u.targetIdx=0,(s??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let f=i[u];if(typeof f!="function")for(let h of f.bindings){a+=h[zg].requiredVars;let m=u+1;h.create&&(h.targetIdx=m,(o??=[]).push(h)),h.update&&(h.targetIdx=m,(s??=[]).push(h))}}let l=[n];if(i)for(let u of i){let f=typeof u=="function"?u:u.type,h=il(f);l.push(h)}return Tv(0,null,uP(o,s),1,a,l,null,null,null,[r],null)}function uP(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function uC(t){let n=t[zg].kind;return n==="input"||n==="twoWay"}var Fu=class extends kE{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=gs(e[U],xe),this.location=Ms(this._tNode,e),this.instance=gn(this._tNode.index,e)[at],this.hostView=this.changeDetectorRef=new gr(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=Hv(i,r[U],r,n,e);this.previousInputValues.set(n,e);let s=gn(i.index,r);nf(s,1)}get injector(){return new vo(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function fP(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var Qe=(()=>{class t{static __NG_ELEMENT_ID__=hP}return t})();function hP(){let t=vt();return zE(t,ie())}var $g=class t extends Qe{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return Ms(this._hostTNode,this._hostLView)}get injector(){return new vo(this._hostTNode,this._hostLView)}get parentInjector(){let n=av(this._hostTNode,this._hostLView);if(VC(n)){let e=Mu(n,this._hostLView),i=Iu(n),r=e[U].data[i+8];return new vo(r,e)}else return new vo(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=fC(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-qe}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=bl(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,r,bo(this._hostTNode,s)),a}createComponent(n,e,i,r,o,s,a){let l=n&&!sk(n),c;if(l)c=e;else{let E=e||{};c=E.index,i=E.injector,r=E.projectableNodes,o=E.environmentInjector||E.ngModuleRef,s=E.directives,a=E.bindings}let u=l?n:new vr(In(n)),f=i||this.parentInjector;if(!o&&u.ngModule==null){let I=(l?f:this.parentInjector).get(Ne,null);I&&(o=I)}let h=In(u.componentType??{}),m=bl(this._lContainer,h?.id??null),p=m?.firstChild??null,w=u.create(f,r,p,o,s,a);return this.insertImpl(w.hostView,c,bo(this._hostTNode,m)),w}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(hw(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let l=r[st],c=new t(l,l[Ut],l[st]);c.detach(c.indexOf(n))}}let o=this._adjustIndex(e),s=this._lContainer;return ks(s,r,o,i),n.attachToViewContainerRef(),Mp(fg(s),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=fC(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=yl(this._lContainer,e);i&&(rl(fg(this._lContainer),e),Ju(i[U],i))}detach(n){let e=this._adjustIndex(n,-1),i=yl(this._lContainer,e);return i&&rl(fg(this._lContainer),e)!=null?new gr(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function fC(t){return t[sl]}function fg(t){return t[sl]||(t[sl]=[])}function zE(t,n){let e,i=n[t.index];return en(i)?e=i:(e=wE(i,n,null,t),n[t.index]=e,Rv(n,e)),$E(e,n,t,i),new $g(e,t,n)}function mP(t,n){let e=t[Ee],i=e.createComment(""),r=pn(n,t),o=e.parentNode(r);return ku(e,o,i,e.nextSibling(r),!1),i}var $E=GE,Zv=()=>!1;function pP(t,n,e){return Zv(t,n,e)}function GE(t,n,e,i){if(t[ni])return;let r;e.type&8?r=tn(i):r=mP(n,e),t[ni]=r}function gP(t,n,e){if(t[ni]&&t[An])return!0;let i=e[Ht],r=n.index-xe;if(!i||QC(n)||R0(i,r))return!1;let s=Mg(i,r),a=i.data[Gu]?.[r];if(a===void 0)return!1;let[l,c]=BN(s,a);return t[ni]=l,t[An]=c,!0}function vP(t,n,e,i){Zv(t,e,n)||GE(t,n,e,i)}function WE(){$E=vP,Zv=gP}var Gg=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},Wg=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)Qv(n,e).matches!==null&&this.queries[e].setDirty()}},Lu=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=wP(n):this.predicate=n}},qg=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},Yg=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,_P(e,o)),this.matchTNodeWithReadOption(n,e,Du(e,n,o,!1,!1))}else i===je?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,Du(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===O||r===Qe||r===je&&e.type&4)this.addMatch(e.index,-2);else{let o=Du(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function _P(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function yP(t,n){return t.type&11?Ms(t,n):t.type&4?rf(t,n):null}function bP(t,n,e,i){return e===-1?yP(n,t):e===-2?DP(t,n,i):pl(t,t[U],e,n)}function DP(t,n,e){if(e===O)return Ms(n,t);if(e===je)return rf(n,t);if(e===Qe)return zE(n,t)}function qE(t,n,e,i){let r=n[ti].queries[i];if(r.matches===null){let o=t.data,s=e.matches,a=[];for(let l=0;s!==null&&l<s.length;l+=2){let c=s[l];if(c<0)a.push(null);else{let u=o[c];a.push(bP(n,u,s[l+1],e.metadata.read))}}r.matches=a}return r.matches}function Zg(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let s=qE(t,n,r,e);for(let a=0;a<o.length;a+=2){let l=o[a];if(l>0)i.push(s[a/2]);else{let c=o[a+1],u=n[-l];for(let f=qe;f<u.length;f++){let h=u[f];h[ur]===h[st]&&Zg(h[U],h,c,i)}if(u[ho]!==null){let f=u[ho];for(let h=0;h<f.length;h++){let m=f[h];Zg(m[U],m,c,i)}}}}}return i}function Kv(t,n){return t[ti].queries[n].queryList}function YE(t,n,e){let i=new Pn((e&4)===4);return gw(t,n,i,i.destroy),(n[ti]??=new Wg).queries.push(new Gg(i))-1}function ZE(t,n,e){let i=Ue();return i.firstCreatePass&&(QE(i,new Lu(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),YE(i,ie(),n)}function KE(t,n,e,i){let r=Ue();if(r.firstCreatePass){let o=vt();QE(r,new Lu(n,e,i),o.index),CP(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return YE(r,ie(),e)}function wP(t){return t.split(",").map(n=>n.trim())}function QE(t,n,e){t.queries===null&&(t.queries=new qg),t.queries.track(new Yg(n,e))}function CP(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function Qv(t,n){return t.queries.getByIndex(n)}function XE(t,n){let e=t[U],i=Qv(e,n);return i.crossesNgTemplate?Zg(e,t,n,[]):qE(e,t,i,n)}function JE(t,n,e){let i,r=Fa(()=>{i._dirtyCounter();let o=EP(i,t);if(n&&o===void 0)throw new S(-951,!1);return o});return i=r[it],i._dirtyCounter=k(0),i._flatValue=void 0,r}function Xv(t){return JE(!0,!1,t)}function Jv(t){return JE(!0,!0,t)}function ex(t,n){let e=t[it];e._lView=ie(),e._queryIndex=n,e._queryList=Kv(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function EP(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[se]&4)return n?void 0:kt;let r=Kv(e,i),o=XE(e,i);return r.reset(o,qC),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}var ci=class{},uf=class{};var Vu=class extends ci{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new Pu(this);constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=Ep(n);this._bootstrapComponents=Z0(o.bootstrap),this._r3Injector=tg(n,e,[{provide:ci,useValue:this},{provide:Rl,useValue:this.componentFactoryResolver},...i],tl(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},ju=class extends uf{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new Vu(this.moduleType,n,[])}};var Dl=class extends ci{injector;componentFactoryResolver=new Pu(this);instance=null;constructor(n){super();let e=new ao([...n.providers,{provide:ci,useValue:this},{provide:Rl,useValue:this.componentFactoryResolver}],n.parent||fs(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function Ns(t,n,e=null){return new Dl({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var xP=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=Jd(!1,e.type),r=i.length>0?Ns([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=b({token:t,providedIn:"environment",factory:()=>new t(K(Ne))})}return t})();function T(t){return Cl(()=>{let n=tx(t),e=te(y({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection===lv.OnPush,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(xP).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||Fn.Emulated,styles:t.styles||kt,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&yn("NgStandalone"),nx(e);let i=t.dependencies;return e.directiveDefs=hC(i,SP),e.pipeDefs=hC(i,xp),e.id=TP(e),e})}function SP(t){return In(t)||il(t)}function R(t){return Cl(()=>({type:t.type,bootstrap:t.bootstrap||kt,declarations:t.declarations||kt,imports:t.imports||kt,exports:t.exports||kt,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function IP(t,n){if(t==null)return Mn;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,s,a,l;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,l=r[3]||null):(o=r,s=r,a=Ku.None,l=null),e[o]=[i,a,l],n[o]=s}return e}function MP(t){if(t==null)return Mn;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function C(t){return Cl(()=>{let n=tx(t);return nx(n),n})}function e_(t){return{type:t.type,name:t.name,factory:null,pure:t.pure!==!1,standalone:t.standalone??!0,onDestroy:t.type.prototype.ngOnDestroy||null}}function tx(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Mn,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||kt,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:IP(t.inputs,n),outputs:MP(t.outputs),debugInfo:null}}function nx(t){t.features?.forEach(n=>n(t))}function hC(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function TP(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}function t_(t){let n=e=>{let i=Array.isArray(t);e.hostDirectives===null?(e.resolveHostDirectives=AP,e.hostDirectives=i?t.map(Kg):[t]):i?e.hostDirectives.unshift(...t.map(Kg)):e.hostDirectives.unshift(t)};return n.ngInherit=!0,n}function AP(t){let n=[],e=!1,i=null,r=null;for(let o=0;o<t.length;o++){let s=t[o];if(s.hostDirectives!==null){let a=n.length;i??=new Map,r??=new Map,ix(s,n,i),r.set(s,[a,n.length-1])}o===0&&Rn(s)&&(e=!0,n.push(s))}for(let o=e?1:0;o<t.length;o++)n.push(t[o]);return[n,i,r]}function ix(t,n,e){if(t.hostDirectives!==null)for(let i of t.hostDirectives)if(typeof i=="function"){let r=i();for(let o of r)mC(Kg(o),n,e)}else mC(i,n,e)}function mC(t,n,e){let i=il(t.directive);RP(i.declaredInputs,t.inputs),ix(i,n,e),e.set(i,t),n.push(i)}function Kg(t){return typeof t=="function"?{directive:gt(t),inputs:Mn,outputs:Mn}:{directive:gt(t.directive),inputs:pC(t.inputs),outputs:pC(t.outputs)}}function pC(t){if(t===void 0||t.length===0)return Mn;let n={};for(let e=0;e<t.length;e+=2)n[t[e]]=t[e+1];return n}function RP(t,n){for(let e in n)if(n.hasOwnProperty(e)){let i=n[e],r=t[e];t[i]=r}}function kP(t){return Object.getPrototypeOf(t.prototype).constructor}function oe(t){let n=kP(t.type),e=!0,i=[t];for(;n;){let r;if(Rn(t))r=n.\u0275cmp||n.\u0275dir;else{if(n.\u0275cmp)throw new S(903,!1);r=n.\u0275dir}if(r){if(e){i.push(r);let s=t;s.inputs=hg(t.inputs),s.declaredInputs=hg(t.declaredInputs),s.outputs=hg(t.outputs);let a=r.hostBindings;a&&LP(t,a);let l=r.viewQuery,c=r.contentQueries;if(l&&PP(t,l),c&&FP(t,c),OP(t,r),KD(t.outputs,r.outputs),Rn(r)&&r.data.animation){let u=t.data;u.animation=(u.animation||[]).concat(r.data.animation)}}let o=r.features;if(o)for(let s=0;s<o.length;s++){let a=o[s];a&&a.ngInherit&&a(t),a===oe&&(e=!1)}}n=Object.getPrototypeOf(n)}NP(i)}function OP(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function NP(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=Es(r.hostAttrs,e=Es(e,r.hostAttrs))}}function hg(t){return t===Mn?{}:t===kt?[]:t}function PP(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function FP(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function LP(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function rx(t,n,e,i,r,o,s,a){if(e.firstCreatePass){t.mergedAttrs=Es(t.mergedAttrs,t.attrs);let u=t.tView=Tv(2,t,r,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),u.queries=e.queries.embeddedTView(t))}a&&(t.flags|=a),vs(t,!1);let l=ox(e,n,t,i);uu()&&Fv(e,n,l,t),xs(l,n);let c=wE(l,n,l,t);n[i+xe]=c,Rv(n,c),pP(c,t,n)}function VP(t,n,e,i,r,o,s,a,l,c,u){let f=e+xe,h;return n.firstCreatePass?(h=Os(n,f,4,s||null,a||null),iu()&&NE(n,t,h,nn(n.consts,c),Vv),PC(n,h)):h=n.data[f],rx(h,t,n,e,i,r,o,l),ps(h)&&tf(n,t,h),c!=null&&Al(t,h,u),h}function Ss(t,n,e,i,r,o,s,a,l,c,u){let f=e+xe,h;if(n.firstCreatePass){if(h=Os(n,f,4,s||null,a||null),c!=null){let m=nn(n.consts,c);h.localNames=[];for(let p=0;p<m.length;p+=2)h.localNames.push(m[p],-1)}}else h=n.data[f];return rx(h,t,n,e,i,r,o,l),c!=null&&Al(t,h,u),h}function Me(t,n,e,i,r,o,s,a){let l=ie(),c=Ue(),u=nn(c.consts,o);return VP(l,c,t,n,e,i,r,u,void 0,s,a),Me}function ff(t,n,e,i,r,o,s,a){let l=ie(),c=Ue(),u=nn(c.consts,o);return Ss(l,c,t,n,e,i,r,u,void 0,s,a),ff}var ox=sx;function sx(t,n,e,i){return Mi(!0),n[Ee].createComment("")}function jP(t,n,e,i){let r=!Yu(n,e);Mi(r);let o=n[Ht]?.data[s0]?.[i]??null;if(o!==null&&e.tView!==null&&e.tView.ssrId===null&&(e.tView.ssrId=o),r)return sx(t,n);let s=n[Ht],a=of(s,t,n,e);qu(s,i,a);let l=vv(s,i);return sf(l,a)}function ax(){ox=jP}var on=(function(t){return t[t.NOT_STARTED=0]="NOT_STARTED",t[t.IN_PROGRESS=1]="IN_PROGRESS",t[t.COMPLETE=2]="COMPLETE",t[t.FAILED=3]="FAILED",t})(on||{}),gC=0,BP=1,ut=(function(t){return t[t.Placeholder=0]="Placeholder",t[t.Loading=1]="Loading",t[t.Complete=2]="Complete",t[t.Error=3]="Error",t})(ut||{});var UP=0,kl=1;var HP=4,zP=5;var $P=7,Cs=8,GP=9,n_=(function(t){return t[t.Manual=0]="Manual",t[t.Playthrough=1]="Playthrough",t})(n_||{});function xu(t,n){let e=qP(t),i=n[e];if(i!==null){for(let r of i)r();n[e]=null}}function WP(t){xu(1,t),xu(0,t),xu(2,t)}function qP(t){let n=HP;return t===1?n=zP:t===2&&(n=GP),n}function lx(t){return t+1}function Ps(t,n){let e=t[U],i=lx(n.index);return t[i]}function Ol(t,n){let e=lx(n.index);return t.data[e]}function YP(t,n,e){let i=n[U],r=Ol(i,e);switch(t){case ut.Complete:return r.primaryTmplIndex;case ut.Loading:return r.loadingTmplIndex;case ut.Error:return r.errorTmplIndex;case ut.Placeholder:return r.placeholderTmplIndex;default:return null}}function vC(t,n){return n===ut.Placeholder?t.placeholderBlockConfig?.[gC]??null:n===ut.Loading?t.loadingBlockConfig?.[gC]??null:null}function ZP(t){return t.loadingBlockConfig?.[BP]??null}function _C(t,n){if(!t||t.length===0)return n;let e=new Set(t);for(let i of n)e.add(i);return t.length===e.size?t:Array.from(e)}function KP(t,n){let e=n.primaryTmplIndex+xe;return gs(t,e)}var QP=(()=>{class t{cachedInjectors=new Map;getOrCreateInjector(e,i,r,o){if(!this.cachedInjectors.has(e)){let s=r.length>0?Ns(r,i,o):null;this.cachedInjectors.set(e,s)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=b({token:t,providedIn:"environment",factory:()=>new t})}return t})();var cx=new g("");function mg(t,n,e){return t.get(QP).getOrCreateInjector(n,t,e,"")}function XP(t,n,e){if(t instanceof ws){let r=t.injector,o=t.parentInjector,s=mg(o,n,e);return new ws(r,s)}let i=t.get(Ne);if(i!==t){let r=mg(i,n,e);return new ws(t,r)}return mg(t,n,e)}function go(t,n,e,i=!1){let r=e[st],o=r[U];if(hr(r))return;let s=Ps(r,n),a=s[kl],l=s[$P];if(!(l!==null&&t<l)&&yC(a,t)&&yC(s[UP]??-1,t)){let c=Ol(o,n),f=!i&&!0&&(ZP(c)!==null||vC(c,ut.Loading)!==null||vC(c,ut.Placeholder))?tF:eF;try{f(t,s,e,n,r)}catch(h){Uv(r,h)}}}function JP(t,n){let e=t[An]?.findIndex(r=>r.data[d0]===n[kl])??-1;return{dehydratedView:e>-1?t[An][e]:null,dehydratedViewIx:e}}function eF(t,n,e,i,r){Se(we.DeferBlockStateStart);let o=YP(t,r,i);if(o!==null){n[kl]=t;let s=r[U],a=o+xe,l=gs(s,a),c=0;$v(e,c);let u;if(t===ut.Complete){let p=Ol(s,i),w=p.providers;w&&w.length>0&&(u=XP(r[Xt],p,w))}let{dehydratedView:f,dehydratedViewIx:h}=JP(e,n),m=Rs(r,l,null,{injector:u,dehydratedView:f});if(ks(e,m,c,bo(l,f)),nf(m,2),h>-1&&e[An]?.splice(h,1),(t===ut.Complete||t===ut.Error)&&Array.isArray(n[Cs])){for(let p of n[Cs])p();n[Cs]=null}}Se(we.DeferBlockStateEnd)}function yC(t,n){return t<n}function bC(t,n,e){t.loadingPromise.then(()=>{t.loadingState===on.COMPLETE?go(ut.Complete,n,e):t.loadingState===on.FAILED&&go(ut.Error,n,e)})}var tF=null;var hf=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();function di(t){return typeof t=="function"&&t[it]!==void 0}function i_(t){return di(t)&&typeof t.set=="function"}var r_=new g("");function _r(t){return!!t&&typeof t.then=="function"}function o_(t){return!!t&&typeof t.subscribe=="function"}var dx=new g("");var s_=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=d(dx,{optional:!0})??[];injector=d(W);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=wt(this.injector,r);if(_r(o))e.push(o);else if(o_(o)){let s=new Promise((a,l)=>{o.subscribe({complete:a,error:l})});e.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),yr=new g("");function ux(){jm(()=>{let t="";throw new S(600,t)})}function fx(t){return t.isBoundToModule}var nF=10;var ht=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=d(rn);afterRenderManager=d(Xu);zonelessEnabled=d(dl);rootEffectScheduler=d(hu);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new x;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=d(si);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(fe(e=>!e))}constructor(){d(Vn,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=d(Ne);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=W.NULL){return this._injector.get(G).run(()=>{Se(we.BootstrapComponentStart);let s=e instanceof cf;if(!this._injector.get(s_).done){let p="";throw new S(405,p)}let l;s?l=e:l=this._injector.get(Rl).resolveComponentFactory(e),this.componentTypes.push(l.componentType);let c=fx(l)?void 0:this._injector.get(ci),u=i||l.selector,f=l.create(r,[],u,c),h=f.location.nativeElement,m=f.injector.get(r_,null);return m?.registerApplication(h),f.onDestroy(()=>{this.detachView(f.hostView),ml(this.components,f),m?.unregisterApplication(h)}),this._loadComponent(f),Se(we.BootstrapComponentEnd,f),f})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Se(we.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Qu.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Se(we.ChangeDetectionEnd),new S(101,!1);let e=X(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,X(e),this.afterTick.next(),Se(we.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(ft,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<nF;){Se(we.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Se(we.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!al(r))continue;let o=i&&!this.zonelessEnabled?0:1;_E(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>al(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;ml(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(yr,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>ml(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new S(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function ml(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function a_(){let t,n;return{promise:new Promise((i,r)=>{t=i,n=r}),resolve:t,reject:n}}function hx(t,n,e){let i=n[Xt],r=n[U];if(t.loadingState!==on.NOT_STARTED)return t.loadingPromise??Promise.resolve();let o=Ps(n,e),s=KP(r,t);t.loadingState=on.IN_PROGRESS,xu(1,o);let a=t.dependencyResolverFn,l=i.get(ys).add();return a?(t.loadingPromise=Promise.allSettled(a()).then(c=>{let u=!1,f=null,h=[],m=[];for(let p=0;p<c.length;p++){let w=c[p];if(w.status==="fulfilled"){let E=w.value,I=In(E)||il(E);if(I)h.push(I);else{let ce=xp(E);ce&&m.push(ce)}}else{u=!0,f=w.reason instanceof Error?w.reason:new Error(String(w.reason));break}}if(u){if(t.loadingState=on.FAILED,t.errorTmplIndex===null){let w="",E=new S(-750,w);Uv(n,E)}}else{t.loadingState=on.COMPLETE;let p=s.tView;if(h.length>0){p.directiveRegistry=_C(p.directiveRegistry,h);let w=h.map(I=>I.type),E=Jd(!1,...w);t.providers=E}m.length>0&&(p.pipeRegistry=_C(p.pipeRegistry,m))}}),t.loadingPromise.finally(()=>{t.loadingPromise=null,l()})):(t.loadingPromise=Promise.resolve().then(()=>{t.loadingPromise=null,t.loadingState=on.COMPLETE,l()}),t.loadingPromise)}function iF(t,n){return n[Xt].get(cx,null,{optional:!0})?.behavior!==n_.Manual}function rF(t,n,e){let i=n[U],r=n[e.index];if(!iF(t,n))return;let o=Ps(n,e),s=Ol(i,e);switch(WP(o),s.loadingState){case on.NOT_STARTED:go(ut.Loading,e,r),hx(s,n,e),s.loadingState===on.IN_PROGRESS&&bC(s,e,r);break;case on.IN_PROGRESS:go(ut.Loading,e,r),bC(s,e,r);break;case on.COMPLETE:go(ut.Complete,e,r);break;case on.FAILED:go(ut.Error,e,r);break;default:}}async function mx(t,n,e){let i=t.get(Wu);if(i.hydrating.has(n))return;let{parentBlockPromise:o,hydrationQueue:s}=$k(n,t);if(s.length===0)return;o!==null&&s.shift(),aF(i,s),o!==null&&await o;let a=s[0];i.has(a)?await DC(t,s,e):i.awaitParentBlock(a,async()=>await DC(t,s,e))}async function DC(t,n,e){let i=t.get(Wu),r=i.hydrating,o=t.get(si),s=o.add();for(let l=0;l<n.length;l++){let c=n[l],u=i.get(c);if(u!=null){if(await cF(u),await lF(t),oF(u)){VN(u),wC(n.slice(l),i);break}r.get(c).resolve()}else{sF(l,n,i),wC(n.slice(l),i);break}}let a=n[n.length-1];await r.get(a)?.promise,o.remove(s),e&&e(n),jN(i.get(a),n,i,t.get(ht))}function oF(t){return Ps(t.lView,t.tNode)[kl]===ut.Error}function sF(t,n,e){let i=t-1,r=i>-1?e.get(n[i]):null;r&&lf(r.lContainer)}function wC(t,n){let e=n.hydrating;for(let i in t)e.get(i)?.reject();n.cleanup(t)}function aF(t,n){for(let e of n)t.hydrating.set(e,a_())}function lF(t){return new Promise(n=>Ke(n,{injector:t}))}async function cF(t){let{tNode:n,lView:e}=t,i=Ps(e,n);return new Promise(r=>{dF(i,r),rF(2,e,n)})}function dF(t,n){Array.isArray(t[Cs])||(t[Cs]=[]),t[Cs].push(n)}function le(t,n,e,i){let r=ie(),o=mr();if(vn(r,o,n)){let s=Ue(),a=cl();sN(a,r,t,n,e,i)}return le}var Qg=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let s=this.detach(i);this.attach(i,o),this.attach(r,s)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function pg(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function uF(t,n,e,i){let r,o,s=0,a=t.length-1,l=void 0;if(Array.isArray(n)){X(i);let c=n.length-1;for(X(null);s<=a&&s<=c;){let u=t.at(s),f=n[s],h=pg(s,u,s,f,e);if(h!==0){h<0&&t.updateValue(s,f),s++;continue}let m=t.at(a),p=n[c],w=pg(a,m,c,p,e);if(w!==0){w<0&&t.updateValue(a,p),a--,c--;continue}let E=e(s,u),I=e(a,m),ce=e(s,f);if(Object.is(ce,I)){let nt=e(c,p);Object.is(nt,E)?(t.swap(s,a),t.updateValue(a,p),c--,a--):t.move(a,s),t.updateValue(s,f),s++;continue}if(r??=new Bu,o??=EC(t,s,a,e),Xg(t,r,s,ce))t.updateValue(s,f),s++,a++;else if(o.has(ce))r.set(E,t.detach(s)),a--;else{let nt=t.create(s,n[s]);t.attach(s,nt),s++,a++}}for(;s<=c;)CC(t,r,e,s,n[s]),s++}else if(n!=null){X(i);let c=n[Symbol.iterator]();X(null);let u=c.next();for(;!u.done&&s<=a;){let f=t.at(s),h=u.value,m=pg(s,f,s,h,e);if(m!==0)m<0&&t.updateValue(s,h),s++,u=c.next();else{r??=new Bu,o??=EC(t,s,a,e);let p=e(s,h);if(Xg(t,r,s,p))t.updateValue(s,h),s++,a++,u=c.next();else if(!o.has(p))t.attach(s,t.create(s,h)),s++,a++,u=c.next();else{let w=e(s,f);r.set(w,t.detach(s)),a--}}}for(;!u.done;)CC(t,r,e,t.length,u.value),u=c.next()}for(;s<=a;)t.destroy(t.detach(a--));r?.forEach(c=>{t.destroy(c)})}function Xg(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function CC(t,n,e,i,r){if(Xg(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function EC(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var Bu=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function z(t,n,e,i,r,o,s,a){yn("NgControlFlow");let l=ie(),c=Ue(),u=nn(c.consts,o);return Ss(l,c,t,n,e,i,r,u,256,s,a),l_}function l_(t,n,e,i,r,o,s,a){yn("NgControlFlow");let l=ie(),c=Ue(),u=nn(c.consts,o);return Ss(l,c,t,n,e,i,r,u,512,s,a),l_}function $(t,n){yn("NgControlFlow");let e=ie(),i=mr(),r=e[i]!==$t?e[i]:-1,o=r!==-1?Uu(e,xe+r):void 0,s=0;if(vn(e,i,t)){let a=X(null);try{if(o!==void 0&&$v(o,s),t!==-1){let l=xe+t,c=Uu(e,l),u=nv(e[U],l),f=AE(c,u,e),h=Rs(e,u,n,{dehydratedView:f});ks(c,h,s,bo(u,f))}}finally{X(a)}}else if(o!==void 0){let a=CE(o,s);a!==void 0&&(a[at]=n)}}var Jg=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-qe}};function Oi(t,n){return n}var ev=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function Ni(t,n,e,i,r,o,s,a,l,c,u,f,h){yn("NgControlFlow");let m=ie(),p=Ue(),w=l!==void 0,E=ie(),I=a?s.bind(E[St][at]):s,ce=new ev(w,I);E[xe+t]=ce,Ss(m,p,t+1,n,e,i,r,nn(p.consts,o),256),w&&Ss(m,p,t+2,l,c,u,f,nn(p.consts,h),512)}var tv=class extends Qg{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-qe}at(n){return this.getLView(n)[at].$implicit}attach(n,e){let i=e[Ht];this.needsIndexUpdate||=n!==this.length,ks(this.lContainer,e,n,bo(this.templateTNode,i)),fF(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,hF(this.lContainer,n),mF(this.lContainer,n)}create(n,e){let i=bl(this.lContainer,this.templateTNode.tView.ssrId);return Rs(this.hostLView,this.templateTNode,new Jg(this.lContainer,e,n),{dehydratedView:i})}destroy(n){Ju(n[U],n)}updateValue(n,e){this.getLView(n)[at].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[at].$index=n}getLView(n){return pF(this.lContainer,n)}};function Pi(t){let n=X(null),e=ri();try{let i=ie(),r=i[U],o=i[e],s=e+1,a=Uu(i,s);if(o.liveCollection===void 0){let c=nv(r,s);o.liveCollection=new tv(a,i,c)}else o.liveCollection.reset();let l=o.liveCollection;if(uF(l,t,o.trackByFn,n),l.updateIndexes(),o.hasEmptyBlock){let c=mr(),u=l.length===0;if(vn(i,c,u)){let f=e+2,h=Uu(i,f);if(u){let m=nv(r,f),p=AE(h,m,i),w=Rs(i,m,void 0,{dehydratedView:p});ks(h,w,0,bo(m,p))}else r.firstUpdatePass&&af(h),$v(h,0)}}}finally{X(n)}}function Uu(t,n){return t[n]}function fF(t,n){if(t.length<=qe)return;let e=qe+n,i=t[e],r=i?i[fr]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[Xt];VO(o,r),yo.delete(i[Ii]),r.detachedLeaveAnimationFns=void 0}}function hF(t,n){if(t.length<=qe)return;let e=qe+n,i=t[e],r=i?i[fr]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function mF(t,n){return yl(t,n)}function pF(t,n){return CE(t,n)}function nv(t,n){return gs(t,n)}function re(t,n,e){let i=ie(),r=mr();if(vn(i,r,n)){let o=Ue(),s=cl();uE(s,i,t,n,i[Ee],e)}return re}function iv(t,n,e,i,r){Hv(n,t,e,r?"class":"style",i)}function v(t,n,e,i){let r=ie(),o=r[U],s=t+xe,a=o.firstCreatePass?Wv(s,r,2,n,Vv,iu(),e,i):o.data[s];if(ii(a)){let l=r[Tn].tracingService;if(l&&l.componentCreate){let c=o.data[a.directiveStart+a.componentOffset];return l.componentCreate(UE(c),()=>(xC(t,n,r,a,i),v))}}return xC(t,n,r,a,i),v}function xC(t,n,e,i,r){if(jv(i,e,t,n,c_),ps(i)){let o=e[U];tf(o,e,i),_v(o,i,e)}r!=null&&Al(e,i)}function _(){let t=Ue(),n=vt(),e=Bv(n);return t.firstCreatePass&&qv(t,e),Wp(e)&&qp(),Gp(),e.classesWithoutHost!=null&&fk(e)&&iv(t,e,ie(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&hk(e)&&iv(t,e,ie(),e.stylesWithoutHost,!1),_}function he(t,n,e,i){return v(t,n,e,i),_(),he}function lt(t,n,e,i){let r=ie(),o=r[U],s=t+xe,a=o.firstCreatePass?eP(s,o,2,n,e,i):o.data[s];return jv(a,r,t,n,c_),i!=null&&Al(r,a),lt}function yt(){let t=vt(),n=Bv(t);return Wp(n)&&qp(),Gp(),yt}function Ot(t,n,e,i){return lt(t,n,e,i),yt(),Ot}var c_=(t,n,e,i,r)=>(Mi(!0),xv(n[Ee],i,du()));function gF(t,n,e,i,r){let o=!Yu(n,e);if(Mi(o),o)return xv(n[Ee],i,du());let s=n[Ht],a=of(s,t,n,e);return A0(s,r)&&qu(s,r,a.nextSibling),s&&(ZC(e)||KC(a))&&ii(e)&&(yw(e),z0(a)),a}function px(){c_=gF}function It(t,n,e){let i=ie(),r=i[U],o=t+xe,s=r.firstCreatePass?Wv(o,i,8,"ng-container",Vv,iu(),n,e):r.data[o];if(jv(s,i,t,"ng-container",gx),ps(s)){let a=i[U];tf(a,i,s),_v(a,s,i)}return e!=null&&Al(i,s),It}function Mt(){let t=Ue(),n=vt(),e=Bv(n);return t.firstCreatePass&&qv(t,e),Mt}function Nt(t,n,e){return It(t,n,e),Mt(),Nt}var gx=(t,n,e,i,r)=>(Mi(!0),U0(n[Ee],""));function vF(t,n,e,i,r){let o,s=!Yu(n,e);if(Mi(s),s)return U0(n[Ee],"");let a=n[Ht],l=of(a,t,n,e),c=Hk(a,r);return qu(a,r,l),o=sf(c,l),o}function vx(){gx=vF}function Ct(){return ie()}function Gt(t,n,e){let i=ie(),r=mr();if(vn(i,r,n)){let o=Ue(),s=cl();fE(s,i,t,n,i[Ee],e)}return Gt}var ul=void 0;function _F(t){let n=Math.floor(Math.abs(t)),e=t.toString().replace(/^[^.]*\.?/,"").length;return n===1&&e===0?1:5}var yF=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],ul,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],ul,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",ul,ul,ul],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",_F],gg={};function Nl(t){let n=bF(t),e=SC(n);if(e)return e;let i=n.split("-")[0];if(e=SC(i),e)return e;if(i==="en")return yF;throw new S(701,!1)}function SC(t){return t in gg||(gg[t]=fn.ng&&fn.ng.common&&fn.ng.common.locales&&fn.ng.common.locales[t]),gg[t]}var br=(function(t){return t[t.LocaleId=0]="LocaleId",t[t.DayPeriodsFormat=1]="DayPeriodsFormat",t[t.DayPeriodsStandalone=2]="DayPeriodsStandalone",t[t.DaysFormat=3]="DaysFormat",t[t.DaysStandalone=4]="DaysStandalone",t[t.MonthsFormat=5]="MonthsFormat",t[t.MonthsStandalone=6]="MonthsStandalone",t[t.Eras=7]="Eras",t[t.FirstDayOfWeek=8]="FirstDayOfWeek",t[t.WeekendRange=9]="WeekendRange",t[t.DateFormat=10]="DateFormat",t[t.TimeFormat=11]="TimeFormat",t[t.DateTimeFormat=12]="DateTimeFormat",t[t.NumberSymbols=13]="NumberSymbols",t[t.NumberFormats=14]="NumberFormats",t[t.CurrencyCode=15]="CurrencyCode",t[t.CurrencySymbol=16]="CurrencySymbol",t[t.CurrencyName=17]="CurrencyName",t[t.Currencies=18]="Currencies",t[t.Directionality=19]="Directionality",t[t.PluralCase=20]="PluralCase",t[t.ExtraData=21]="ExtraData",t})(br||{});function bF(t){return t.toLowerCase().replace(/_/g,"-")}var Pl="en-US",DF="USD";var wF=Pl;function _x(t){typeof t=="string"&&(wF=t.toLowerCase().replace(/_/g,"-"))}function J(t,n,e){let i=ie(),r=Ue(),o=vt();return yx(r,i,i[Ee],o,t,n,e),J}function mf(t,n,e){let i=ie(),r=Ue(),o=vt();return(o.type&3||e)&&jE(o,r,i,e,i[Ee],t,n,Eu(o,i,n)),mf}function yx(t,n,e,i,r,o,s){let a=!0,l=null;if((i.type&3||s)&&(l??=Eu(i,n,o),jE(i,t,n,s,e,r,o,l)&&(a=!1)),a){let c=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let f=0;f<u.length;f+=2){let h=u[f],m=u[f+1];l??=Eu(i,n,o),dC(i,n,h,m,r,l)}if(c&&c.length)for(let f of c)l??=Eu(i,n,o),dC(i,n,f,r,r,l)}}function V(t=1){return Rw(t)}function CF(t,n){let e=null,i=CO(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?Q0(t,o,!0):SO(i,o))return r}return e}function De(t){let n=ie()[St][Ut];if(!n.projection){let e=t?t.length:1,i=n.projection=nw(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=t?CF(o,t):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function H(t,n=0,e,i,r,o){let s=ie(),a=Ue(),l=i?t+1:null;l!==null&&Ss(s,a,l,i,r,o,null,e);let c=Os(a,xe+t,16,null,e||null);c.projection===null&&(c.projection=n),Kp();let f=!s[Ht]||ru();s[St][Ut].projection[c.projection]===null&&l!==null?EF(s,a,l):f&&!Il(c)&&ZO(a,s,c)}function EF(t,n,e){let i=xe+e,r=n.data[i],o=t[i],s=bl(o,r.tView.ssrId),a=Rs(t,r,void 0,{dehydratedView:s});ks(o,a,0,bo(r,s))}function mt(t,n,e,i){return KE(t,n,e,i),mt}function ct(t,n,e){return ZE(t,n,e),ct}function q(t){let n=ie(),e=Ue(),i=au();ll(i+1);let r=Qv(e,i);if(t.dirty&&fw(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=XE(n,i);t.reset(o,qC),t.notifyOnChanges()}return!0}return!1}function Y(){return Kv(ie(),au())}function pf(t,n,e,i,r){return ex(n,KE(t,e,i,r)),pf}function gf(t,n,e,i){return ex(t,ZE(n,e,i)),gf}function vf(t=1){ll(au()+t)}function ui(t){let n=Dw();return Vp(n,xe+t)}function _u(t,n){return t<<17|n<<2}function Do(t){return t>>17&32767}function xF(t){return(t&2)==2}function SF(t,n){return t&131071|n<<17}function rv(t){return t|2}function Is(t){return(t&131068)>>2}function vg(t,n){return t&-131069|n<<2}function IF(t){return(t&1)===1}function ov(t){return t|1}function MF(t,n,e,i,r,o){let s=o?n.classBindings:n.styleBindings,a=Do(s),l=Is(s);t[i]=e;let c=!1,u;if(Array.isArray(e)){let f=e;u=f[1],(u===null||us(f,u)>0)&&(c=!0)}else u=e;if(r)if(l!==0){let h=Do(t[a+1]);t[i+1]=_u(h,a),h!==0&&(t[h+1]=vg(t[h+1],i)),t[a+1]=SF(t[a+1],i)}else t[i+1]=_u(a,0),a!==0&&(t[a+1]=vg(t[a+1],i)),a=i;else t[i+1]=_u(l,0),a===0?a=i:t[l+1]=vg(t[l+1],i),l=i;c&&(t[i+1]=rv(t[i+1])),IC(t,u,i,!0),IC(t,u,i,!1),TF(n,u,t,i,o),s=_u(a,l),o?n.classBindings=s:n.styleBindings=s}function TF(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&us(o,n)>=0&&(e[i+1]=ov(e[i+1]))}function IC(t,n,e,i){let r=t[e+1],o=n===null,s=i?Do(r):Is(r),a=!1;for(;s!==0&&(a===!1||o);){let l=t[s],c=t[s+1];AF(l,n)&&(a=!0,t[s+1]=i?ov(c):rv(c)),s=i?Do(c):Is(c)}a&&(t[e+1]=i?rv(r):ov(r))}function AF(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?us(t,n)>=0:!1}var Nn={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function RF(t){return t.substring(Nn.key,Nn.keyEnd)}function kF(t){return OF(t),bx(t,Dx(t,0,Nn.textEnd))}function bx(t,n){let e=Nn.textEnd;return e===n?-1:(n=Nn.keyEnd=NF(t,Nn.key=n,e),Dx(t,n,e))}function OF(t){Nn.key=0,Nn.keyEnd=0,Nn.value=0,Nn.valueEnd=0,Nn.textEnd=t.length}function Dx(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function NF(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function Re(t,n,e){return wx(t,n,e,!1),Re}function Z(t,n){return wx(t,n,null,!0),Z}function sn(t){FF(HF,PF,t,!0)}function PF(t,n){for(let e=kF(n);e>=0;e=bx(n,e))Qd(t,RF(n),!0)}function wx(t,n,e,i){let r=ie(),o=Ue(),s=ou(2);if(o.firstUpdatePass&&Ex(o,t,s,i),n!==$t&&vn(r,s,n)){let a=o.data[ri()];xx(o,a,r,r[Ee],t,r[s+1]=$F(n,e),i,s)}}function FF(t,n,e,i){let r=Ue(),o=ou(2);r.firstUpdatePass&&Ex(r,null,o,i);let s=ie();if(e!==$t&&vn(s,o,e)){let a=r.data[ri()];if(Sx(a,i)&&!Cx(r,o)){let l=i?a.classesWithoutHost:a.stylesWithoutHost;l!==null&&(e=Wd(l,e||"")),iv(r,a,s,e,i)}else zF(r,a,s,s[Ee],s[o+1],s[o+1]=UF(t,n,e),i,o)}}function Cx(t,n){return n>=t.expandoStartIndex}function Ex(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[ri()],s=Cx(t,e);Sx(o,i)&&n===null&&!s&&(n=!1),n=LF(r,o,n,i),MF(r,o,n,e,s,i)}}function LF(t,n,e,i){let r=Iw(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=_g(null,t,n,e,i),e=wl(e,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||t[s]!==r)if(e=_g(r,t,n,e,i),o===null){let l=VF(t,n,i);l!==void 0&&Array.isArray(l)&&(l=_g(null,t,n,l[1],i),l=wl(l,n.attrs,i),jF(t,n,i,l))}else o=BF(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function VF(t,n,e){let i=e?n.classBindings:n.styleBindings;if(Is(i)!==0)return t[Do(i)]}function jF(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[Do(r)]=i}function BF(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=t[o].hostAttrs;i=wl(i,s,e)}return wl(i,n.attrs,e)}function _g(t,n,e,i,r){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=n[a],i=wl(i,o.hostAttrs,r),o!==t);)a++;return t!==null&&(e.directiveStylingLast=a),i}function wl(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),Qd(t,s,e?!0:n[++o]))}return t===void 0?null:t}function UF(t,n,e){if(e==null||e==="")return kt;let i=[],r=Ln(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)r.hasOwnProperty(o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function HF(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&Qd(t,i,e)}function zF(t,n,e,i,r,o,s,a){r===$t&&(r=kt);let l=0,c=0,u=0<r.length?r[0]:null,f=0<o.length?o[0]:null;for(;u!==null||f!==null;){let h=l<r.length?r[l+1]:void 0,m=c<o.length?o[c+1]:void 0,p=null,w;u===f?(l+=2,c+=2,h!==m&&(p=f,w=m)):f===null||u!==null&&u<f?(l+=2,p=u):(c+=2,p=f,w=m),p!==null&&xx(t,n,e,i,p,w,s,a),u=l<r.length?r[l]:null,f=c<o.length?o[c]:null}}function xx(t,n,e,i,r,o,s,a){if(!(n.type&3))return;let l=t.data,c=l[a+1],u=IF(c)?MC(l,n,e,r,Is(c),s):void 0;if(!Hu(u)){Hu(o)||xF(c)&&(o=MC(l,null,e,r,a,s));let f=Lp(ri(),e);QO(i,s,f,r,o)}}function MC(t,n,e,i,r,o){let s=n===null,a;for(;r>0;){let l=t[r],c=Array.isArray(l),u=c?l[1]:l,f=u===null,h=e[r+1];h===$t&&(h=f?kt:void 0);let m=f?Xd(h,i):u===i?h:void 0;if(c&&!Hu(m)&&(m=Xd(l,i)),Hu(m)&&(a=m,s))return a;let p=t[r+1];r=s?Do(p):Is(p)}if(n!==null){let l=o?n.residualClasses:n.residualStyles;l!=null&&(a=Xd(l,i))}return a}function Hu(t){return t!==void 0}function $F(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=tl(Ln(t)))),t}function Sx(t,n){return(t.flags&(n?8:16))!==0}function M(t,n=""){let e=ie(),i=Ue(),r=t+xe,o=i.firstCreatePass?Os(i,r,1,n,null):i.data[r],s=Ix(i,e,o,n);e[r]=s,uu()&&Fv(i,e,s,o),vs(o,!1)}var Ix=(t,n,e,i)=>(Mi(!0),B0(n[Ee],i));function GF(t,n,e,i){let r=!Yu(n,e);if(Mi(r),r)return B0(n[Ee],i);let o=n[Ht];return of(o,t,n,e)}function Mx(){Ix=GF}function WF(t,n,e,i=""){return vn(t,mr(),e)?n+ds(e)+i:$t}function qF(t,n,e,i,r,o=""){let s=ww(),a=nP(t,s,e,r);return ou(2),a?n+ds(e)+i+ds(r)+o:$t}function et(t){return $e("",t),et}function $e(t,n,e){let i=ie(),r=WF(i,t,n,e);return r!==$t&&Tx(i,ri(),r),$e}function _f(t,n,e,i,r){let o=ie(),s=qF(o,t,n,e,i,r);return s!==$t&&Tx(o,ri(),s),_f}function Tx(t,n,e){let i=Lp(n,t);hO(t[Ee],i,e)}function yf(t,n,e){i_(n)&&(n=n());let i=ie(),r=mr();if(vn(i,r,n)){let o=Ue(),s=cl();uE(s,i,t,n,i[Ee],e)}return yf}function d_(t,n){let e=i_(t);return e&&t.set(n),e}function bf(t,n){let e=ie(),i=Ue(),r=vt();return yx(i,e,e[Ee],r,t,n),bf}function TC(t,n,e){let i=Ue();i.firstCreatePass&&Ax(n,i.data,i.blueprint,Rn(t),e)}function Ax(t,n,e,i,r){if(t=gt(t),Array.isArray(t))for(let o=0;o<t.length;o++)Ax(t[o],n,e,i,r);else{let o=Ue(),s=ie(),a=vt(),l=so(t)?t:gt(t.provide),c=kp(t),u=a.providerIndexes&1048575,f=a.directiveStart,h=a.providerIndexes>>20;if(so(t)||!t.multi){let m=new _o(c,r,ne,null),p=bg(l,n,r?u:u+h,f);p===-1?(wg(Au(a,s),o,l),yg(o,t,n.length),n.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(m),s.push(m)):(e[p]=m,s[p]=m)}else{let m=bg(l,n,u+h,f),p=bg(l,n,u,u+h),w=m>=0&&e[m],E=p>=0&&e[p];if(r&&!E||!r&&!w){wg(Au(a,s),o,l);let I=KF(r?ZF:YF,e.length,r,i,c,t);!r&&E&&(e[p].providerFactory=I),yg(o,t,n.length,0),n.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(I),s.push(I)}else{let I=Rx(e[r?p:m],c,!r&&i);yg(o,t,m>-1?m:p,I)}!r&&i&&E&&e[p].componentProviders++}}}function yg(t,n,e,i){let r=so(n),o=lw(n);if(r||o){let l=(o?gt(n.useClass):n).prototype.ngOnDestroy;if(l){let c=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let u=c.indexOf(e);u===-1?c.push(e,[i,l]):c[u+1].push(i,l)}else c.push(e,l)}}}function Rx(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function bg(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function YF(t,n,e,i,r){return sv(this.multi,[])}function ZF(t,n,e,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,l=pl(i,i[U],this.providerFactory.index,r);s=l.slice(0,a),sv(o,s);for(let c=a;c<l.length;c++)s.push(l[c])}else s=[],sv(o,s);return s}function sv(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function KF(t,n,e,i,r,o){let s=new _o(t,e,ne,null);return s.multi=[],s.index=n,s.componentProviders=0,Rx(s,r,i&&!e),s}function _e(t,n){return e=>{e.providersResolver=(i,r)=>TC(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>TC(i,r?r(n):n,!0))}}function Co(t,n){let e=Xp()+t,i=ie();return i[e]===$t?VE(i,e,n()):tP(i,e)}function QF(t,n){let e=t[n];return e===$t?void 0:e}function XF(t,n,e,i,r,o){let s=n+e;return vn(t,s,r)?VE(t,s+1,o?i.call(o,r):i(r)):QF(t,s+1)}function Dr(t,n){let e=Ue(),i,r=t+xe;e.firstCreatePass?(i=JF(n,e.pipeRegistry),e.data[r]=i,i.onDestroy&&(e.destroyHooks??=[]).push(r,i.onDestroy)):i=e.data[r];let o=i.factory||(i.factory=lr(i.type,!0)),s,a=Vt(ne);try{let l=Tu(!1),c=o();return Tu(l),jp(e,ie(),r,c),c}finally{Vt(a)}}function JF(t,n){if(n)for(let e=n.length-1;e>=0;e--){let i=n[e];if(t===i.name)return i}}function wr(t,n,e){let i=t+xe,r=ie(),o=Vp(r,i);return e1(r,i)?XF(r,Xp(),n,o.transform,e,o):o.transform(e)}function e1(t,n){return t[U].data[n].pure}function u_(t,n){return rf(t,n)}var zu=class{ngModuleFactory;componentFactories;constructor(n,e){this.ngModuleFactory=n,this.componentFactories=e}},f_=(()=>{class t{compileModuleSync(e){return new ju(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}compileModuleAndAllComponentsSync(e){let i=this.compileModuleSync(e),r=Ep(e),o=Z0(r.declarations).reduce((s,a)=>{let l=In(a);return l&&s.push(new vr(l)),s},[]);return new zu(i,o)}compileModuleAndAllComponentsAsync(e){return Promise.resolve(this.compileModuleAndAllComponentsSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var kx=(()=>{class t{applicationErrorHandler=d(rn);appRef=d(ht);taskService=d(si);ngZone=d(G);zonelessEnabled=d(dl);tracing=d(Vn,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new ae;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Ja):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(d(sg,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?Pw:ng;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Ja+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Ox(){return[{provide:Sn,useExisting:kx},{provide:G,useClass:el},{provide:dl,useValue:!0}]}function t1(){return typeof $localize<"u"&&$localize.locale||Pl}var Fl=new g("",{factory:()=>d(Fl,{optional:!0,skipSelf:!0})||t1()}),h_=new g("",{factory:()=>DF});function Te(t){return GD(t)}function Pt(t,n){return Fa(t,n?.equal)}var n1=t=>t;function Df(t,n){if(typeof t=="function"){let e=rp(t,n1,n?.equal);return Nx(e,n?.debugName)}else{let e=rp(t.source,t.computation,t.equal);return Nx(e,t.debugName)}}function Nx(t,n){let e=t[it],i=t;return i.set=r=>zD(e,r),i.update=r=>$D(e,r),i.asReadonly=fu.bind(t),i}var __={JSACTION:"__jsaction",OWNER:"__owner"},Vx={};function i1(t){return t[__.JSACTION]}function Px(t,n){t[__.JSACTION]=n}function r1(t){return Vx[t]}function o1(t,n){Vx[t]=n}var ee={CLICK:"click",CLICKMOD:"clickmod",DBLCLICK:"dblclick",FOCUS:"focus",FOCUSIN:"focusin",BLUR:"blur",FOCUSOUT:"focusout",SUBMIT:"submit",KEYDOWN:"keydown",KEYPRESS:"keypress",KEYUP:"keyup",MOUSEOVER:"mouseover",MOUSEOUT:"mouseout",MOUSEENTER:"mouseenter",MOUSELEAVE:"mouseleave",POINTEROVER:"pointerover",POINTEROUT:"pointerout",POINTERENTER:"pointerenter",POINTERLEAVE:"pointerleave",ERROR:"error",LOAD:"load",TOUCHSTART:"touchstart",TOUCHEND:"touchend",TOUCHMOVE:"touchmove",TOGGLE:"toggle"},s1=[ee.MOUSEENTER,ee.MOUSELEAVE,"pointerenter","pointerleave"],l9=[ee.CLICK,ee.DBLCLICK,ee.FOCUSIN,ee.FOCUSOUT,ee.KEYDOWN,ee.KEYUP,ee.KEYPRESS,ee.MOUSEOVER,ee.MOUSEOUT,ee.SUBMIT,ee.TOUCHSTART,ee.TOUCHEND,ee.TOUCHMOVE,"touchcancel","auxclick","change","compositionstart","compositionupdate","compositionend","beforeinput","input","select","copy","cut","paste","mousedown","mouseup","wheel","contextmenu","dragover","dragenter","dragleave","drop","dragstart","dragend","pointerdown","pointermove","pointerup","pointercancel","pointerover","pointerout","gotpointercapture","lostpointercapture","ended","loadedmetadata","pagehide","pageshow","visibilitychange","beforematch"],a1=[ee.FOCUS,ee.BLUR,ee.ERROR,ee.LOAD,ee.TOGGLE],y_=t=>a1.indexOf(t)>=0;function l1(t){return t===ee.MOUSEENTER?ee.MOUSEOVER:t===ee.MOUSELEAVE?ee.MOUSEOUT:t===ee.POINTERENTER?ee.POINTEROVER:t===ee.POINTERLEAVE?ee.POINTEROUT:t}function c1(t,n,e,i){let r=!1;y_(n)&&(r=!0);let o=typeof i=="boolean"?{capture:r,passive:i}:r;return t.addEventListener(n,e,o),{eventType:n,handler:e,capture:r,passive:i}}function d1(t,n){if(t.removeEventListener){let e=typeof n.passive=="boolean"?{capture:n.capture}:n.capture;t.removeEventListener(n.eventType,n.handler,e)}else t.detachEvent&&t.detachEvent(`on${n.eventType}`,n.handler)}function u1(t){t.preventDefault?t.preventDefault():t.returnValue=!1}var Fx=typeof navigator<"u"&&/Macintosh/.test(navigator.userAgent);function f1(t){return t.which===2||t.which==null&&t.button===4}function h1(t){return Fx&&t.metaKey||!Fx&&t.ctrlKey||f1(t)||t.shiftKey}function m1(t,n,e){let i=t.relatedTarget;return(t.type===ee.MOUSEOVER&&n===ee.MOUSEENTER||t.type===ee.MOUSEOUT&&n===ee.MOUSELEAVE||t.type===ee.POINTEROVER&&n===ee.POINTERENTER||t.type===ee.POINTEROUT&&n===ee.POINTERLEAVE)&&(!i||i!==e&&!e.contains(i))}function p1(t,n){let e={};for(let i in t){if(i==="srcElement"||i==="target")continue;let r=i,o=t[r];typeof o!="function"&&(e[r]=o)}return t.type===ee.MOUSEOVER?e.type=ee.MOUSEENTER:t.type===ee.MOUSEOUT?e.type=ee.MOUSELEAVE:t.type===ee.POINTEROVER?e.type=ee.POINTERENTER:e.type=ee.POINTERLEAVE,e.target=e.srcElement=n,e.bubbles=!1,e._originalEvent=t,e}var xf=class{element;handlerInfos=[];constructor(n){this.element=n}addEventListener(n,e,i){this.handlerInfos.push(c1(this.element,n,e(this.element),i))}cleanUp(){for(let n=0;n<this.handlerInfos.length;n++)d1(this.element,this.handlerInfos[n]);this.handlerInfos=[]}},g1={EVENT_ACTION_SEPARATOR:":"};function Cr(t){return t.eventType}function b_(t,n){t.eventType=n}function Cf(t){return t.event}function jx(t,n){t.event=n}function Bx(t){return t.targetElement}function Ux(t,n){t.targetElement=n}function Hx(t){return t.eic}function v1(t,n){t.eic=n}function _1(t){return t.timeStamp}function y1(t,n){t.timeStamp=n}function Ef(t){return t.eia}function zx(t,n,e){t.eia=[n,e]}function m_(t){t.eia=void 0}function wf(t){return t[1]}function b1(t){return t.eirp}function $x(t,n){t.eirp=n}function Gx(t){return t.eir}function Wx(t,n){t.eir=n}function qx(t){return{eventType:t.eventType,event:t.event,targetElement:t.targetElement,eic:t.eic,eia:t.eia,timeStamp:t.timeStamp,eirp:t.eirp,eiack:t.eiack,eir:t.eir}}function D1(t,n,e,i,r,o,s,a){return{eventType:t,event:n,targetElement:e,eic:i,timeStamp:r,eia:o,eirp:s,eiack:a}}var p_=class t{eventInfo;constructor(n){this.eventInfo=n}getEventType(){return Cr(this.eventInfo)}setEventType(n){b_(this.eventInfo,n)}getEvent(){return Cf(this.eventInfo)}setEvent(n){jx(this.eventInfo,n)}getTargetElement(){return Bx(this.eventInfo)}setTargetElement(n){Ux(this.eventInfo,n)}getContainer(){return Hx(this.eventInfo)}setContainer(n){v1(this.eventInfo,n)}getTimestamp(){return _1(this.eventInfo)}setTimestamp(n){y1(this.eventInfo,n)}getAction(){let n=Ef(this.eventInfo);if(n)return{name:n[0],element:n[1]}}setAction(n){if(!n){m_(this.eventInfo);return}zx(this.eventInfo,n.name,n.element)}getIsReplay(){return b1(this.eventInfo)}setIsReplay(n){$x(this.eventInfo,n)}getResolved(){return Gx(this.eventInfo)}setResolved(n){Wx(this.eventInfo,n)}clone(){return new t(qx(this.eventInfo))}},w1={},C1=/\s*;\s*/,E1=ee.CLICK,g_=class{a11yClickSupport=!1;clickModSupport=!0;syntheticMouseEventSupport;updateEventInfoForA11yClick=void 0;preventDefaultForA11yClick=void 0;populateClickOnlyAction=void 0;constructor({syntheticMouseEventSupport:n=!1,clickModSupport:e=!0}={}){this.syntheticMouseEventSupport=n,this.clickModSupport=e}resolveEventType(n){this.clickModSupport&&Cr(n)===ee.CLICK&&h1(Cf(n))?b_(n,ee.CLICKMOD):this.a11yClickSupport&&this.updateEventInfoForA11yClick(n)}resolveAction(n){Gx(n)||(this.populateAction(n,Bx(n)),Wx(n,!0))}resolveParentAction(n){let e=Ef(n),i=e&&wf(e);m_(n);let r=i&&this.getParentNode(i);r&&this.populateAction(n,r)}populateAction(n,e){let i=e;for(;i&&i!==Hx(n)&&(i.nodeType===Node.ELEMENT_NODE&&this.populateActionOnElement(i,n),!Ef(n));)i=this.getParentNode(i);let r=Ef(n);if(r&&(this.a11yClickSupport&&this.preventDefaultForA11yClick(n),this.syntheticMouseEventSupport&&(Cr(n)===ee.MOUSEENTER||Cr(n)===ee.MOUSELEAVE||Cr(n)===ee.POINTERENTER||Cr(n)===ee.POINTERLEAVE)))if(m1(Cf(n),Cr(n),wf(r))){let o=p1(Cf(n),wf(r));jx(n,o),Ux(n,wf(r))}else m_(n)}getParentNode(n){let e=n[__.OWNER];if(e)return e;let i=n.parentNode;return i?.nodeName==="#document-fragment"?i?.host??null:i}populateActionOnElement(n,e){let i=this.parseActions(n),r=i[Cr(e)];r!==void 0&&zx(e,r,n),this.a11yClickSupport&&this.populateClickOnlyAction(n,e,i)}parseActions(n){let e=i1(n);if(!e){let i=n.getAttribute(mu.JSACTION);if(!i)e=w1,Px(n,e);else{if(e=r1(i),!e){e={};let r=i.split(C1);for(let o=0;o<r.length;o++){let s=r[o];if(!s)continue;let a=s.indexOf(g1.EVENT_ACTION_SEPARATOR),l=a!==-1,c=l?s.substr(0,a).trim():E1,u=l?s.substr(a+1).trim():s;e[c]=u}o1(i,e)}Px(n,e)}}return e}addA11yClickSupport(n,e,i){this.a11yClickSupport=!0,this.updateEventInfoForA11yClick=n,this.preventDefaultForA11yClick=e,this.populateClickOnlyAction=i}},Yx=(function(t){return t[t.I_AM_THE_JSACTION_FRAMEWORK=0]="I_AM_THE_JSACTION_FRAMEWORK",t})(Yx||{}),v_=class{dispatchDelegate;actionResolver;eventReplayer;eventReplayScheduled=!1;replayEventInfoWrappers=[];constructor(n,{actionResolver:e,eventReplayer:i}={}){this.dispatchDelegate=n,this.actionResolver=e,this.eventReplayer=i}dispatch(n){let e=new p_(n);this.actionResolver?.resolveEventType(n),this.actionResolver?.resolveAction(n);let i=e.getAction();if(i&&x1(i.element,e)&&u1(e.getEvent()),this.eventReplayer&&e.getIsReplay()){this.scheduleEventInfoWrapperReplay(e);return}this.dispatchDelegate(e)}scheduleEventInfoWrapperReplay(n){this.replayEventInfoWrappers.push(n),!this.eventReplayScheduled&&(this.eventReplayScheduled=!0,Promise.resolve().then(()=>{this.eventReplayScheduled=!1,this.eventReplayer(this.replayEventInfoWrappers)}))}};function x1(t,n){return t.tagName==="A"&&(n.getEventType()===ee.CLICK||n.getEventType()===ee.CLICKMOD)}var Zx=Symbol.for("propagationStopped"),D_={REPLAY:101};var S1="`preventDefault` called during event replay.";var I1="`composedPath` called during event replay.",Sf=class{dispatchDelegate;clickModSupport;actionResolver;dispatcher;constructor(n,e=!0){this.dispatchDelegate=n,this.clickModSupport=e,this.actionResolver=new g_({clickModSupport:e}),this.dispatcher=new v_(i=>{this.dispatchToDelegate(i)},{actionResolver:this.actionResolver})}dispatch(n){this.dispatcher.dispatch(n)}dispatchToDelegate(n){for(n.getIsReplay()&&A1(n),M1(n);n.getAction();){if(R1(n),y_(n.getEventType())&&n.getAction().element!==n.getTargetElement()||(this.dispatchDelegate(n.getEvent(),n.getAction().name),T1(n)))return;this.actionResolver.resolveParentAction(n.eventInfo)}}};function M1(t){let n=t.getEvent(),e=t.getEvent().stopPropagation.bind(n),i=()=>{n[Zx]=!0,e()};Eo(n,"stopPropagation",i),Eo(n,"stopImmediatePropagation",i)}function T1(t){return!!t.getEvent()[Zx]}function A1(t){let n=t.getEvent(),e=t.getTargetElement(),i=n.preventDefault.bind(n);Eo(n,"target",e),Eo(n,"eventPhase",D_.REPLAY),Eo(n,"preventDefault",()=>{throw i(),new Error(S1+"")}),Eo(n,"composedPath",()=>{throw new Error(I1+"")})}function R1(t){let n=t.getEvent(),e=t.getAction()?.element;e&&Eo(n,"currentTarget",e,{configurable:!0})}function Eo(t,n,e,{configurable:i=!1}={}){Object.defineProperty(t,n,{value:e,configurable:i})}function Kx(t,n){t.ecrd(e=>{n.dispatch(e)},Yx.I_AM_THE_JSACTION_FRAMEWORK)}function k1(t){return t?.q??[]}function O1(t){t&&(Lx(t.c,t.et,t.h),Lx(t.c,t.etc,t.h,!0))}function Lx(t,n,e,i){for(let r=0;r<n.length;r++)t.removeEventListener(n[r],e,i)}var N1=!1,Qx=(()=>{class t{static MOUSE_SPECIAL_SUPPORT=N1;containerManager;eventHandlers={};browserEventTypeToExtraEventTypes={};dispatcher=null;queuedEventInfos=[];constructor(e){this.containerManager=e}handleEvent(e,i,r){let o=D1(e,i,i.target,r,Date.now());this.handleEventInfo(o)}handleEventInfo(e){if(!this.dispatcher){$x(e,!0),this.queuedEventInfos?.push(e);return}this.dispatcher(e)}addEvent(e,i,r){if(e in this.eventHandlers||!this.containerManager||!t.MOUSE_SPECIAL_SUPPORT&&s1.indexOf(e)>=0)return;let o=(a,l,c)=>{this.handleEvent(a,l,c)};this.eventHandlers[e]=o;let s=l1(i||e);if(s!==e){let a=this.browserEventTypeToExtraEventTypes[s]||[];a.push(e),this.browserEventTypeToExtraEventTypes[s]=a}this.containerManager.addEventListener(s,a=>l=>{o(e,l,a)},r)}replayEarlyEvents(e=window._ejsa){e&&(this.replayEarlyEventInfos(e.q),O1(e),delete window._ejsa)}replayEarlyEventInfos(e){for(let i=0;i<e.length;i++){let r=e[i],o=this.getEventTypesForBrowserEventType(r.eventType);for(let s=0;s<o.length;s++){let a=qx(r);b_(a,o[s]),this.handleEventInfo(a)}}}getEventTypesForBrowserEventType(e){let i=[];return this.eventHandlers[e]&&i.push(e),this.browserEventTypeToExtraEventTypes[e]&&i.push(...this.browserEventTypeToExtraEventTypes[e]),i}handler(e){return this.eventHandlers[e]}cleanUp(){this.containerManager?.cleanUp(),this.containerManager=null,this.eventHandlers={},this.browserEventTypeToExtraEventTypes={},this.dispatcher=null,this.queuedEventInfos=[]}registerDispatcher(e,i){this.ecrd(e,i)}ecrd(e,i){if(this.dispatcher=e,this.queuedEventInfos?.length){for(let r=0;r<this.queuedEventInfos.length;r++)this.handleEventInfo(this.queuedEventInfos[r]);this.queuedEventInfos=null}}}return t})();function Xx(t,n=window){return k1(n._ejsas?.[t])}function w_(t,n=window){n._ejsas&&(n._ejsas[t]=void 0)}var cS=Symbol("InputSignalNode#UNSET"),Z1=te(y({},La),{transformFn:void 0,applyValueToInputSignal(t,n){Zr(t,n)}});function dS(t,n){let e=Object.create(Z1);e.value=t,e.transformFn=n?.transform;function i(){if(Ji(e),e.value===cS){let r=null;throw new S(-950,r)}return e.value}return i[it]=e,i}var Zt=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>El(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function Jx(t,n){return dS(t,n)}function K1(t){return dS(cS,t)}var uS=(Jx.required=K1,Jx);function eS(t,n){return Xv(n)}function Q1(t,n){return Jv(n)}var jl=(eS.required=Q1,eS);function tS(t,n){return Xv(n)}function X1(t,n){return Jv(n)}var fS=(tS.required=X1,tS);var E_=new g(""),J1=new g("");function Ll(t){return!t.moduleRef}function eL(t){let n=Ll(t)?t.r3Injector:t.moduleRef.injector,e=n.get(G);return e.run(()=>{Ll(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(rn),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),Ll(t)){let o=()=>n.destroy(),s=t.platformInjector.get(E_);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>t.moduleRef.destroy(),s=t.platformInjector.get(E_);s.add(o),t.moduleRef.onDestroy(()=>{ml(t.allPlatformModules,t.moduleRef),r.unsubscribe(),s.delete(o)})}return nL(i,e,()=>{let o=n.get(si),s=o.add(),a=n.get(s_);return a.runInitializers(),a.donePromise.then(()=>{let l=n.get(Fl,Pl);if(_x(l||Pl),!n.get(J1,!0))return Ll(t)?n.get(ht):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(Ll(t)){let u=n.get(ht);return t.rootComponent!==void 0&&u.bootstrap(t.rootComponent),u}else return tL?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(s)})})})}var tL;function nL(t,n,e){try{let i=e();return _r(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var Mf=null;function iL(t=[],n){return W.create({name:n,providers:[{provide:ol,useValue:"platform"},{provide:E_,useValue:new Set([()=>Mf=null])},...t]})}function rL(t=[]){if(Mf)return Mf;let n=iL(t);return Mf=n,ux(),oL(n),n}function oL(t){let n=t.get($u,null);wt(t,()=>{n?.forEach(e=>e())})}var If=new WeakSet,nS="";function iS(t){return t.get(hv,h0)}function hS(){let t=[{provide:hv,useFactory:()=>{let n=!0;{let e=d(_n);n=!!window._ejsas?.[e]}return n&&yn("NgEventReplay"),n}}];return t.push({provide:Jn,useValue:()=>{let n=d(ht),{injector:e}=n;if(!If.has(n)){let i=d(mv);if(iS(e)){w0();let r=e.get(_n),o=b0(r,(s,a,l)=>{s.nodeType===Node.ELEMENT_NODE&&(g0(s,a,l),v0(s,i))});n.onDestroy(o)}}},multi:!0},{provide:yr,useFactory:()=>{let n=d(ht),{injector:e}=n;return()=>{if(!iS(e)||If.has(n))return;If.add(n);let i=e.get(_n);n.onDestroy(()=>{If.delete(n),w_(i)}),n.whenStable().then(()=>{if(n.destroyed)return;let r=e.get(y0);sL(r,e);let o=e.get(mv);o.get(nS)?.forEach(_0),o.delete(nS);let s=r.instance;T0(e)?n.onDestroy(()=>s.cleanUp()):s.cleanUp()})}},multi:!0}),t}var sL=(t,n)=>{let e=n.get(_n),i=window._ejsas[e],r=t.instance=new Qx(new xf(i.c));for(let a of i.et)r.addEvent(a);for(let a of i.etc)r.addEvent(a);let o=Xx(e);r.replayEarlyEventInfos(o),w_(e);let s=new Sf(a=>{aL(n,a,a.currentTarget)});Kx(r,s)};function aL(t,n,e){let i=(e&&e.getAttribute(Sl))??"";/d\d+/.test(i)?lL(i,t,n,e):n.eventPhase===D_.REPLAY&&pv(n,e)}function lL(t,n,e,i){let r=n.get(m0);r.push({event:e,currentTarget:i}),mx(n,t,cL(r))}function cL(t){return n=>{let e=new Set(n),i=[];for(let{event:r,currentTarget:o}of t){let s=o.getAttribute(Sl);e.has(s)?pv(r,o):i.push({event:r,currentTarget:o})}t.length=0,t.push(...i)}}var rS=!1;var dL=1e4;function uL(){rS||(rS=!0,S0(),px(),Mx(),vx(),ax(),WE(),TE(),dE())}function fL(t){return t.whenStable()}function mS(){let t=[{provide:As,useFactory:()=>{let n=!0;return n=!!d(Ts,{optional:!0})?.get(gv,null),n&&yn("NgHydration"),n}},{provide:Jn,useValue:()=>{xE(!1);let n=d(P);d(As)&&(k0(n),uL())},multi:!0}];return t.push({provide:fv,useFactory:()=>d(As)},{provide:yr,useFactory:()=>{let n=d(Sn);if(d(As)){let e=d(ht);return()=>{fL(e).then(()=>{e.destroyed||(Gv(e),n.notify(7))})}}return()=>{}},multi:!0}),ei(t)}var UK=dL-1e3;var Pe=(()=>{class t{static __NG_ELEMENT_ID__=hL}return t})();function hL(t){return mL(vt(),ie(),(t&16)===16)}function mL(t,n,e){if(ii(t)&&!e){let i=gn(t.index,n);return new gr(i,i)}else if(t.type&175){let i=n[St];return new gr(i,n)}return null}var x_=class{supports(n){return Yv(n)}create(n){return new S_(n)}},pL=(t,n)=>n,S_=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(n){this._trackByFn=n||pL}forEachItem(n){let e;for(e=this._itHead;e!==null;e=e._next)n(e)}forEachOperation(n){let e=this._itHead,i=this._removalsHead,r=0,o=null;for(;e||i;){let s=!i||e&&e.currentIndex<oS(i,r,o)?e:i,a=oS(s,r,o),l=s.currentIndex;if(s===i)r--,i=i._nextRemoved;else if(e=e._next,s.previousIndex==null)r++;else{o||(o=[]);let c=a-r,u=l-r;if(c!=u){for(let h=0;h<c;h++){let m=h<o.length?o[h]:o[h]=0,p=m+h;u<=p&&p<c&&(o[h]=m+1)}let f=s.previousIndex;o[f]=u-c}}a!==l&&n(s,a,l)}}forEachPreviousItem(n){let e;for(e=this._previousItHead;e!==null;e=e._nextPrevious)n(e)}forEachAddedItem(n){let e;for(e=this._additionsHead;e!==null;e=e._nextAdded)n(e)}forEachMovedItem(n){let e;for(e=this._movesHead;e!==null;e=e._nextMoved)n(e)}forEachRemovedItem(n){let e;for(e=this._removalsHead;e!==null;e=e._nextRemoved)n(e)}forEachIdentityChange(n){let e;for(e=this._identityChangesHead;e!==null;e=e._nextIdentityChange)n(e)}diff(n){if(n==null&&(n=[]),!Yv(n))throw new S(900,!1);return this.check(n)?this:null}onDestroy(){}check(n){this._reset();let e=this._itHead,i=!1,r,o,s;if(Array.isArray(n)){this.length=n.length;for(let a=0;a<this.length;a++)o=n[a],s=this._trackByFn(a,o),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,o,s,a),i=!0):(i&&(e=this._verifyReinsertion(e,o,s,a)),Object.is(e.item,o)||this._addIdentityChange(e,o)),e=e._next}else r=0,FE(n,a=>{s=this._trackByFn(r,a),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,a,s,r),i=!0):(i&&(e=this._verifyReinsertion(e,a,s,r)),Object.is(e.item,a)||this._addIdentityChange(e,a)),e=e._next,r++}),this.length=r;return this._truncate(e),this.collection=n,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let n;for(n=this._previousItHead=this._itHead;n!==null;n=n._next)n._nextPrevious=n._next;for(n=this._additionsHead;n!==null;n=n._nextAdded)n.previousIndex=n.currentIndex;for(this._additionsHead=this._additionsTail=null,n=this._movesHead;n!==null;n=n._nextMoved)n.previousIndex=n.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(n,e,i,r){let o;return n===null?o=this._itTail:(o=n._prev,this._remove(n)),n=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._reinsertAfter(n,o,r)):(n=this._linkedRecords===null?null:this._linkedRecords.get(i,r),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._moveAfter(n,o,r)):n=this._addAfter(new I_(e,i),o,r)),n}_verifyReinsertion(n,e,i,r){let o=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return o!==null?n=this._reinsertAfter(o,n._prev,r):n.currentIndex!=r&&(n.currentIndex=r,this._addToMoves(n,r)),n}_truncate(n){for(;n!==null;){let e=n._next;this._addToRemovals(this._unlink(n)),n=e}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(n,e,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(n);let r=n._prevRemoved,o=n._nextRemoved;return r===null?this._removalsHead=o:r._nextRemoved=o,o===null?this._removalsTail=r:o._prevRemoved=r,this._insertAfter(n,e,i),this._addToMoves(n,i),n}_moveAfter(n,e,i){return this._unlink(n),this._insertAfter(n,e,i),this._addToMoves(n,i),n}_addAfter(n,e,i){return this._insertAfter(n,e,i),this._additionsTail===null?this._additionsTail=this._additionsHead=n:this._additionsTail=this._additionsTail._nextAdded=n,n}_insertAfter(n,e,i){let r=e===null?this._itHead:e._next;return n._next=r,n._prev=e,r===null?this._itTail=n:r._prev=n,e===null?this._itHead=n:e._next=n,this._linkedRecords===null&&(this._linkedRecords=new Tf),this._linkedRecords.put(n),n.currentIndex=i,n}_remove(n){return this._addToRemovals(this._unlink(n))}_unlink(n){this._linkedRecords!==null&&this._linkedRecords.remove(n);let e=n._prev,i=n._next;return e===null?this._itHead=i:e._next=i,i===null?this._itTail=e:i._prev=e,n}_addToMoves(n,e){return n.previousIndex===e||(this._movesTail===null?this._movesTail=this._movesHead=n:this._movesTail=this._movesTail._nextMoved=n),n}_addToRemovals(n){return this._unlinkedRecords===null&&(this._unlinkedRecords=new Tf),this._unlinkedRecords.put(n),n.currentIndex=null,n._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=n,n._prevRemoved=null):(n._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=n),n}_addIdentityChange(n,e){return n.item=e,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=n:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=n,n}},I_=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(n,e){this.item=n,this.trackById=e}},M_=class{_head=null;_tail=null;add(n){this._head===null?(this._head=this._tail=n,n._nextDup=null,n._prevDup=null):(this._tail._nextDup=n,n._prevDup=this._tail,n._nextDup=null,this._tail=n)}get(n,e){let i;for(i=this._head;i!==null;i=i._nextDup)if((e===null||e<=i.currentIndex)&&Object.is(i.trackById,n))return i;return null}remove(n){let e=n._prevDup,i=n._nextDup;return e===null?this._head=i:e._nextDup=i,i===null?this._tail=e:i._prevDup=e,this._head===null}},Tf=class{map=new Map;put(n){let e=n.trackById,i=this.map.get(e);i||(i=new M_,this.map.set(e,i)),i.add(n)}get(n,e){let i=n,r=this.map.get(i);return r?r.get(n,e):null}remove(n){let e=n.trackById;return this.map.get(e).remove(n)&&this.map.delete(e),n}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function oS(t,n,e){let i=t.previousIndex;if(i===null)return i;let r=0;return e&&i<e.length&&(r=e[i]),i+n+r}function sS(){return new Fi([new x_])}var Fi=(()=>{class t{factories;static \u0275prov=b({token:t,providedIn:"root",factory:sS});constructor(e){this.factories=e}static create(e,i){if(i!=null){let r=i.factories.slice();e=e.concat(r)}return new t(e)}static extend(e){return{provide:t,useFactory:()=>{let i=d(t,{optional:!0,skipSelf:!0});return t.create(e,i||sS())}}}find(e){let i=this.factories.find(r=>r.supports(e));if(i!=null)return i;throw new S(901,!1)}}return t})();function pS(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;Se(we.BootstrapApplicationStart);try{let o=r?.injector??rL(i),s=[Ox(),Lw,...e||[]],a=new Dl({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return eL({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{Se(we.BootstrapApplicationEnd)}}function L(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function Li(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var C_=Symbol("NOT_SET"),gS=new Set,gL=te(y({},La),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:C_,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==C_&&!Jo(this))return this.signal;try{for(let r of this.cleanup??gS)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=Ci(this),i;try{i=this.userFn.apply(null,n)}finally{er(this,e)}return(this.value===C_||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),T_=class extends vl{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(zt),s),this.scheduler=r;for(let a of Ov){let l=e[a];if(l===void 0)continue;let c=Object.create(gL);c.sequence=this,c.phase=a,c.userFn=l,c.dirty=!0,c.signal=()=>(Ji(c),c.value),c.signal[it]=c,c.registerCleanupFn=u=>(c.cleanup??=new Set).add(u),this.nodes[a]=c,this.hooks[a]=u=>c.phaseFn(u)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??gS)e()}finally{tr(n)}}};function vS(t,n){let e=n?.injector??d(W),i=e.get(Sn),r=e.get(Xu),o=e.get(Vn,null,{optional:!0});r.impl??=e.get(Nv);let s=t;typeof s=="function"&&(s={mixedReadWrite:t});let a=e.get(_s,null,{optional:!0}),l=new T_(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,e,o?.snapshot(null));return r.impl.register(l),l}function Af(t,n){let e=In(t),i=n.elementInjector||fs();return new vr(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}function _S(t){let n=In(t);if(!n)return null;let e=new vr(n);return{get selector(){return e.selector},get type(){return e.componentType},get inputs(){return e.inputs},get outputs(){return e.outputs},get ngContentSelectors(){return e.ngContentSelectors},get isStandalone(){return n.standalone},get isSignal(){return n.signals}}}var yS=null;function bn(){return yS}function A_(t){yS??=t}var Bl=class{},Rf=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:()=>d(bS),providedIn:"platform"})}return t})();var bS=(()=>{class t extends Rf{_location;_history;_doc=d(P);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return bn().getBaseHref(this._doc)}onPopState(e){let i=bn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=bn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function CS(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function DS(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function Er(t){return t&&t[0]!=="?"?`?${t}`:t}var Fs=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:()=>d(_L),providedIn:"root"})}return t})(),vL=new g(""),_L=(()=>{class t extends Fs{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??d(P).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return CS(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+Er(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let s=this.prepareExternalUrl(r+Er(o));this._platformLocation.pushState(e,i,s)}replaceState(e,i,r,o){let s=this.prepareExternalUrl(r+Er(o));this._platformLocation.replaceState(e,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(K(Rf),K(vL,8))};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var xr=(()=>{class t{_subject=new x;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=DL(DS(wS(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+Er(i))}normalize(e){return t.stripTrailingSlash(bL(this._basePath,wS(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Er(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Er(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=Er;static joinWithSlash=CS;static stripTrailingSlash=DS;static \u0275fac=function(i){return new(i||t)(K(Fs))};static \u0275prov=b({token:t,factory:()=>yL(),providedIn:"root"})}return t})();function yL(){return new xr(K(Fs))}function bL(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function wS(t){return t.replace(/\/index.html$/,"")}function DL(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var SS={ADP:[void 0,void 0,0],AFN:[void 0,"\u060B",0],ALL:[void 0,void 0,0],AMD:[void 0,"\u058F",2],AOA:[void 0,"Kz"],ARS:[void 0,"$"],AUD:["A$","$"],AZN:[void 0,"\u20BC"],BAM:[void 0,"KM"],BBD:[void 0,"$"],BDT:[void 0,"\u09F3"],BHD:[void 0,void 0,3],BIF:[void 0,void 0,0],BMD:[void 0,"$"],BND:[void 0,"$"],BOB:[void 0,"Bs"],BRL:["R$"],BSD:[void 0,"$"],BWP:[void 0,"P"],BYN:[void 0,void 0,2],BYR:[void 0,void 0,0],BZD:[void 0,"$"],CAD:["CA$","$",2],CHF:[void 0,void 0,2],CLF:[void 0,void 0,4],CLP:[void 0,"$",0],CNY:["CN\xA5","\xA5"],COP:[void 0,"$",2],CRC:[void 0,"\u20A1",2],CUC:[void 0,"$"],CUP:[void 0,"$"],CZK:[void 0,"K\u010D",2],DJF:[void 0,void 0,0],DKK:[void 0,"kr",2],DOP:[void 0,"$"],EGP:[void 0,"E\xA3"],ESP:[void 0,"\u20A7",0],EUR:["\u20AC"],FJD:[void 0,"$"],FKP:[void 0,"\xA3"],GBP:["\xA3"],GEL:[void 0,"\u20BE"],GHS:[void 0,"GH\u20B5"],GIP:[void 0,"\xA3"],GNF:[void 0,"FG",0],GTQ:[void 0,"Q"],GYD:[void 0,"$",2],HKD:["HK$","$"],HNL:[void 0,"L"],HRK:[void 0,"kn"],HUF:[void 0,"Ft",2],IDR:[void 0,"Rp",2],ILS:["\u20AA"],INR:["\u20B9"],IQD:[void 0,void 0,0],IRR:[void 0,void 0,0],ISK:[void 0,"kr",0],ITL:[void 0,void 0,0],JMD:[void 0,"$"],JOD:[void 0,void 0,3],JPY:["\xA5",void 0,0],KGS:[void 0,"\u20C0"],KHR:[void 0,"\u17DB"],KMF:[void 0,"CF",0],KPW:[void 0,"\u20A9",0],KRW:["\u20A9",void 0,0],KWD:[void 0,void 0,3],KYD:[void 0,"$"],KZT:[void 0,"\u20B8"],LAK:[void 0,"\u20AD",0],LBP:[void 0,"L\xA3",0],LKR:[void 0,"Rs"],LRD:[void 0,"$"],LTL:[void 0,"Lt"],LUF:[void 0,void 0,0],LVL:[void 0,"Ls"],LYD:[void 0,void 0,3],MGA:[void 0,"Ar",0],MGF:[void 0,void 0,0],MMK:[void 0,"K",0],MNT:[void 0,"\u20AE",2],MRO:[void 0,void 0,0],MUR:[void 0,"Rs",2],MXN:["MX$","$"],MYR:[void 0,"RM"],NAD:[void 0,"$"],NGN:[void 0,"\u20A6"],NIO:[void 0,"C$"],NOK:[void 0,"kr",2],NPR:[void 0,"Rs"],NZD:["NZ$","$"],OMR:[void 0,void 0,3],PHP:["\u20B1"],PKR:[void 0,"Rs",2],PLN:[void 0,"z\u0142"],PYG:[void 0,"\u20B2",0],RON:[void 0,"lei"],RSD:[void 0,void 0,0],RUB:[void 0,"\u20BD"],RWF:[void 0,"RF",0],SBD:[void 0,"$"],SEK:[void 0,"kr",2],SGD:[void 0,"$"],SHP:[void 0,"\xA3"],SLE:[void 0,void 0,2],SLL:[void 0,void 0,0],SOS:[void 0,void 0,0],SRD:[void 0,"$"],SSP:[void 0,"\xA3"],STD:[void 0,void 0,0],STN:[void 0,"Db"],SYP:[void 0,"\xA3",0],THB:[void 0,"\u0E3F"],TMM:[void 0,void 0,0],TND:[void 0,void 0,3],TOP:[void 0,"T$"],TRL:[void 0,void 0,0],TRY:[void 0,"\u20BA"],TTD:[void 0,"$"],TWD:["NT$","$",2],TZS:[void 0,void 0,2],UAH:[void 0,"\u20B4"],UGX:[void 0,void 0,0],USD:["$"],UYI:[void 0,void 0,0],UYU:[void 0,"$"],UYW:[void 0,void 0,4],UZS:[void 0,void 0,2],VEF:[void 0,"Bs",2],VND:["\u20AB",void 0,0],VUV:[void 0,void 0,0],XAF:["FCFA",void 0,0],XCD:["EC$","$"],XCG:["Cg."],XOF:["F\u202FCFA",void 0,0],XPF:["CFPF",void 0,0],XXX:["\xA4"],YER:[void 0,void 0,0],ZAR:[void 0,"R"],ZMK:[void 0,void 0,0],ZMW:[void 0,"ZK"],ZWD:[void 0,void 0,0]},O_=(function(t){return t[t.Decimal=0]="Decimal",t[t.Percent=1]="Percent",t[t.Currency=2]="Currency",t[t.Scientific=3]="Scientific",t})(O_||{});var fi={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function Ls(t,n){let e=Nl(t),i=e[br.NumberSymbols][n];if(typeof i>"u"){if(n===fi.CurrencyDecimal)return e[br.NumberSymbols][fi.Decimal];if(n===fi.CurrencyGroup)return e[br.NumberSymbols][fi.Group]}return i}function IS(t,n){return Nl(t)[br.NumberFormats][n]}function wL(t){return Nl(t)[br.Currencies]}function MS(t,n,e="en"){let i=wL(e)[t]||SS[t]||[],r=i[1];return n==="narrow"&&typeof r=="string"?r:i[0]||t}var CL=2;function TS(t){let n,e=SS[t];return e&&(n=e[2]),typeof n=="number"?n:CL}var EL=/^(\d+)?\.((\d+)(-(\d+))?)?$/,ES=22,kf=".",Ul="0",xL=";",SL=",",R_="#",xS="\xA4";function IL(t,n,e,i,r,o,s=!1){let a="",l=!1;if(!isFinite(t))a=Ls(e,fi.Infinity);else{let c=AL(t);s&&(c=TL(c));let u=n.minInt,f=n.minFrac,h=n.maxFrac;if(o){let ce=o.match(EL);if(ce===null)throw new S(2306,!1);let nt=ce[1],Xe=ce[3],Qi=ce[5];nt!=null&&(u=k_(nt)),Xe!=null&&(f=k_(Xe)),Qi!=null?h=k_(Qi):Xe!=null&&f>h&&(h=f);let wi=100;if(u>wi||f>wi||h>wi)throw new S(2306,!1)}RL(c,f,h);let m=c.digits,p=c.integerLen,w=c.exponent,E=[];for(l=m.every(ce=>!ce);p<u;p++)m.unshift(0);for(;p<0;p++)m.unshift(0);p>0?E=m.splice(p,m.length):(E=m,m=[0]);let I=[];for(m.length>=n.lgSize&&I.unshift(m.splice(-n.lgSize,m.length).join(""));m.length>n.gSize;)I.unshift(m.splice(-n.gSize,m.length).join(""));m.length&&I.unshift(m.join("")),a=I.join(Ls(e,i)),E.length&&(a+=Ls(e,r)+E.join("")),w&&(a+=Ls(e,fi.Exponential)+"+"+w)}return t<0&&!l?a=n.negPre+a+n.negSuf:a=n.posPre+a+n.posSuf,a}function AS(t,n,e,i,r){let o=IS(n,O_.Currency),s=ML(o,Ls(n,fi.MinusSign));return s.minFrac=TS(i),s.maxFrac=s.minFrac,IL(t,s,n,fi.CurrencyGroup,fi.CurrencyDecimal,r).replace(xS,e).replace(xS,"").trim()}function ML(t,n="-"){let e={minInt:1,minFrac:0,maxFrac:0,posPre:"",posSuf:"",negPre:"",negSuf:"",gSize:0,lgSize:0},i=t.split(xL),r=i[0],o=i[1],s=r.indexOf(kf)!==-1?r.split(kf):[r.substring(0,r.lastIndexOf(Ul)+1),r.substring(r.lastIndexOf(Ul)+1)],a=s[0],l=s[1]||"";e.posPre=a.substring(0,a.indexOf(R_));for(let u=0;u<l.length;u++){let f=l.charAt(u);f===Ul?e.minFrac=e.maxFrac=u+1:f===R_?e.maxFrac=u+1:e.posSuf+=f}let c=a.split(SL);if(e.gSize=c[1]?c[1].length:0,e.lgSize=c[2]||c[1]?(c[2]||c[1]).length:0,o){let u=r.length-e.posPre.length-e.posSuf.length,f=o.indexOf(R_);e.negPre=o.substring(0,f).replace(/'/g,""),e.negSuf=o.slice(f+u).replace(/'/g,"")}else e.negPre=n+e.posPre,e.negSuf=e.posSuf;return e}function TL(t){if(t.digits[0]===0)return t;let n=t.digits.length-t.integerLen;return t.exponent?t.exponent+=2:(n===0?t.digits.push(0,0):n===1&&t.digits.push(0),t.integerLen+=2),t}function AL(t){let n=Math.abs(t)+"",e=0,i,r,o,s,a;for((r=n.indexOf(kf))>-1&&(n=n.replace(kf,"")),(o=n.search(/e/i))>0?(r<0&&(r=o),r+=+n.slice(o+1),n=n.substring(0,o)):r<0&&(r=n.length),o=0;n.charAt(o)===Ul;o++);if(o===(a=n.length))i=[0],r=1;else{for(a--;n.charAt(a)===Ul;)a--;for(r-=o,i=[],s=0;o<=a;o++,s++)i[s]=Number(n.charAt(o))}return r>ES&&(i=i.splice(0,ES-1),e=r-1,r=1),{digits:i,exponent:e,integerLen:r}}function RL(t,n,e){if(n>e)throw new S(2307,!1);let i=t.digits,r=i.length-t.integerLen,o=Math.min(Math.max(n,r),e),s=o+t.integerLen,a=i[s];if(s>0){i.splice(Math.max(t.integerLen,s));for(let f=s;f<i.length;f++)i[f]=0}else{r=Math.max(0,r),t.integerLen=1,i.length=Math.max(1,s=o+1),i[0]=0;for(let f=1;f<s;f++)i[f]=0}if(a>=5)if(s-1<0){for(let f=0;f>s;f--)i.unshift(0),t.integerLen++;i.unshift(1),t.integerLen++}else i[s-1]++;for(;r<Math.max(0,o);r++)i.push(0);let l=o!==0,c=n+t.integerLen,u=i.reduceRight(function(f,h,m,p){return h=h+f,p[m]=h<10?h:h-10,l&&(p[m]===0&&m>=c?p.pop():l=!1),h>=10?1:0},0);u&&(i.unshift(u),t.integerLen++)}function k_(t){let n=parseInt(t);if(isNaN(n))throw new S(2305,!1);return n}var N_=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=d(W);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(ne(Qe))};static \u0275dir=C({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Ie]})}return t})();function kL(t,n){return new S(2100,!1)}var Vs=(()=>{class t{_locale;_defaultCurrencyCode;constructor(e,i="USD"){this._locale=e,this._defaultCurrencyCode=i}transform(e,i=this._defaultCurrencyCode,r="symbol",o,s){if(!OL(e))return null;s||=this._locale,typeof r=="boolean"&&(r=r?"symbol":"code");let a=i||this._defaultCurrencyCode;r!=="code"&&(r==="symbol"||r==="symbol-narrow"?a=MS(a,r==="symbol"?"wide":"narrow",s):a=r);try{let l=NL(e);return AS(l,s,a,i,o)}catch(l){throw kL(t,l.message)}}static \u0275fac=function(i){return new(i||t)(ne(Fl,16),ne(h_,16))};static \u0275pipe=e_({name:"currency",type:t,pure:!0})}return t})();function OL(t){return!(t==null||t===""||t!==t)}function NL(t){if(typeof t=="string"&&!isNaN(Number(t)-parseFloat(t)))return Number(t);if(typeof t!="number")throw new S(2309,!1);return t}var js=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({})}return t})();function Of(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var xo=class{};var P_="browser";function Nf(t){return t===P_}var Hl=class{_doc;constructor(n){this._doc=n}manager},Pf=(()=>{class t extends Hl{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(K(P))};static \u0275prov=b({token:t,factory:t.\u0275fac})}return t})(),Vf=new g(""),B_=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(s=>{s.manager=this});let r=e.filter(s=>!(s instanceof Pf));this._plugins=r.slice().reverse();let o=e.find(s=>s instanceof Pf);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new S(5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(K(Vf),K(G))};static \u0275prov=b({token:t,factory:t.\u0275fac})}return t})(),L_="ng-app-id";function RS(t){for(let n of t)n.remove()}function kS(t,n){let e=n.createElement("style");return e.textContent=t,e}function FL(t,n,e,i){let r=t.head?.querySelectorAll(`style[${L_}="${n}"],link[${L_}="${n}"]`);if(r)for(let o of r)o.removeAttribute(L_),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]})}function j_(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var U_=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,FL(e,i,this.inline,this.external),this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,kS);i?.forEach(r=>this.addUsage(r,this.external,j_))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(RS(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])RS(e);this.hosts.clear()}addHost(e){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,kS(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,j_(i,this.doc)))}removeHost(e){this.hosts.delete(e)}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(K(P),K(_n),K(wo,8),K(Ai))};static \u0275prov=b({token:t,factory:t.\u0275fac})}return t})(),V_={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},H_=/%COMP%/g;var NS="%COMP%",LL=`_nghost-${NS}`,VL=`_ngcontent-${NS}`,jL=!0,BL=new g("",{factory:()=>jL});function UL(t){return VL.replace(H_,t)}function HL(t){return LL.replace(H_,t)}function PS(t,n){return n.map(e=>e.replace(H_,t))}var z_=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,s,a,l=null,c=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=l,this.tracingService=c,this.defaultRenderer=new zl(e,s,a,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof Lf?r.applyToHost(e):r instanceof $l&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case Fn.Emulated:o=new Lf(l,c,i,this.appId,u,s,a,f);break;case Fn.ShadowDom:return new Ff(l,e,i,s,a,this.nonce,f,c);case Fn.ExperimentalIsolatedShadowDom:return new Ff(l,e,i,s,a,this.nonce,f);default:o=new $l(l,c,i,u,s,a,f);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(K(B_),K(U_),K(_n),K(BL),K(P),K(G),K(wo),K(Vn,8))};static \u0275prov=b({token:t,factory:t.\u0275fac})}return t})(),zl=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(V_[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(OS(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(OS(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new S(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=V_[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=V_[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){r&(li.DashCase|li.Important)?n.style.setProperty(e,i,r&li.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){i&li.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=bn().getGlobalEventTarget(this.doc,n),!n))throw new S(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function OS(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var Ff=class extends zl{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,s,a,l){super(n,r,o,a),this.hostEl=e,this.sharedStylesHost=l,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let c=i.styles;c=PS(i.id,c);for(let f of c){let h=document.createElement("style");s&&h.setAttribute("nonce",s),h.textContent=f,this.shadowRoot.appendChild(h)}let u=i.getExternalStyles?.();if(u)for(let f of u){let h=j_(f,r);s&&h.setAttribute("nonce",s),this.shadowRoot.appendChild(h)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},$l=class extends zl{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,s,a,l){super(n,o,s,a),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let c=i.styles;this.styles=l?PS(l,c):c,this.styleUrls=i.getExternalStyles?.(l)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&yo.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Lf=class extends $l{contentAttr;hostAttr;constructor(n,e,i,r,o,s,a,l){let c=r+"-"+i.id;super(n,e,i,o,s,a,l,c),this.contentAttr=UL(c),this.hostAttr=HL(c)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var jf=class t extends Bl{supportsDOMEvents=!0;static makeCurrent(){A_(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=zL();return e==null?null:$L(e)}resetBaseElement(){Gl=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return Of(document.cookie,n)}},Gl=null;function zL(){return Gl=Gl||document.head.querySelector("base"),Gl?Gl.getAttribute("href"):null}function $L(t){return new URL(t,document.baseURI).pathname}var GL=(()=>{class t{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac})}return t})(),FS=["alt","control","meta","shift"],WL={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},qL={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},LS=(()=>{class t extends Hl{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let s=t.parseEventName(i),a=t.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>bn().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),FS.forEach(c=>{let u=i.indexOf(c);u>-1&&(i.splice(u,1),s+=c+".")}),s+=o,i.length!=0||o.length===0)return null;let l={};return l.domEventName=r,l.fullKey=s,l}static matchEventFullKeyCode(e,i){let r=WL[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),FS.forEach(s=>{if(s!==r){let a=qL[s];a(e)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(K(P))};static \u0275prov=b({token:t,factory:t.\u0275fac})}return t})();async function $_(t,n,e){let i=y({rootComponent:t},YL(n,e));return pS(i)}function YL(t,n){return{platformRef:n?.platformRef,appProviders:[...JL,...t?.providers??[]],platformProviders:XL}}function ZL(){jf.makeCurrent()}function KL(){return new jt}function QL(){return cv(document),document}var XL=[{provide:Ai,useValue:P_},{provide:$u,useValue:ZL,multi:!0},{provide:P,useFactory:QL}];var JL=[{provide:ol,useValue:"root"},{provide:jt,useFactory:KL},{provide:Vf,useClass:Pf,multi:!0},{provide:Vf,useClass:LS,multi:!0},z_,U_,B_,{provide:ft,useExisting:z_},{provide:xo,useClass:GL},[]];var ji=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(e=>{this.headers.set(e,n.headers.get(e)),this.normalizedNames.set(e,n.normalizedNames.get(e))})}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=(n.op==="a"?this.headers.get(e):void 0)||[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=this.headers.get(e);if(!s)return;s=s.filter(a=>o.indexOf(a)===-1),s.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,s)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var W_=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},q_=class{encodeKey(n){return VS(n)}encodeValue(n){return VS(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function eV(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],l=e.get(s)||[];l.push(a),e.set(s,l)}),e}var tV=/%(\d[a-f0-9])/gi,nV={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function VS(t){return encodeURIComponent(t).replace(tV,(n,e)=>nV[e]??n)}function Bf(t){return`${t}`}var Vi=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new q_,n.fromString){if(n.fromObject)throw new S(2805,!1);this.map=eV(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(Bf):[Bf(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=(n.op==="a"?this.map.get(n.param):void 0)||[];e.push(Bf(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=this.map.get(n.param)||[],r=i.indexOf(Bf(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function iV(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function jS(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function BS(t){return typeof Blob<"u"&&t instanceof Blob}function US(t){return typeof FormData<"u"&&t instanceof FormData}function rV(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var HS="Content-Type",zS="Accept",$S="text/plain",GS="application/json",oV=`${GS}, ${$S}, */*`,Bs=class t{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(iV(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new S(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new ji,this.context??=new W_,!this.params)this.params=new Vi,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e.indexOf("?"),l=a===-1?"?":a<e.length-1?"&":"";this.urlWithParams=e+l+s}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||jS(this.body)||BS(this.body)||US(this.body)||rV(this.body)?this.body:this.body instanceof Vi?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||US(this.body)?null:BS(this.body)?this.body.type||null:jS(this.body)?null:typeof this.body=="string"?$S:this.body instanceof Vi?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?GS:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,l=n.mode||this.mode,c=n.redirect||this.redirect,u=n.credentials||this.credentials,f=n.referrer||this.referrer,h=n.integrity||this.integrity,m=n.referrerPolicy||this.referrerPolicy,p=n.transferCache??this.transferCache,w=n.timeout??this.timeout,E=n.body!==void 0?n.body:this.body,I=n.withCredentials??this.withCredentials,ce=n.reportProgress??this.reportProgress,nt=n.headers||this.headers,Xe=n.params||this.params,Qi=n.context??this.context;return n.setHeaders!==void 0&&(nt=Object.keys(n.setHeaders).reduce((wi,Gr)=>wi.set(Gr,n.setHeaders[Gr]),nt)),n.setParams&&(Xe=Object.keys(n.setParams).reduce((wi,Gr)=>wi.set(Gr,n.setParams[Gr]),Xe)),new t(e,i,E,{params:Xe,headers:nt,context:Qi,reportProgress:ce,responseType:r,withCredentials:I,transferCache:p,keepalive:o,cache:a,priority:s,timeout:w,mode:l,redirect:c,credentials:u,referrer:f,integrity:h,referrerPolicy:m})}},So=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(So||{}),Wl=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new ji,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},Y_=class t extends Wl{constructor(n={}){super(n)}type=So.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},Hs=class t extends Wl{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=So.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},Us=class extends Wl{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},sV=200,aV=204;var lV=/^\)\]\}',?\n/;var cV=(()=>{class t{xhrFactory;tracingService=d(Vn,{optional:!0});constructor(e){this.xhrFactory=e}maybePropagateTrace(e){return this.tracingService?.propagate?this.tracingService.propagate(e):e}handle(e){if(e.method==="JSONP")throw new S(-2800,!1);let i=this.xhrFactory;return B(null).pipe(ot(()=>new de(o=>{let s=i.build();if(s.open(e.method,e.urlWithParams),e.withCredentials&&(s.withCredentials=!0),e.headers.forEach((E,I)=>s.setRequestHeader(E,I.join(","))),e.headers.has(zS)||s.setRequestHeader(zS,oV),!e.headers.has(HS)){let E=e.detectContentTypeHeader();E!==null&&s.setRequestHeader(HS,E)}if(e.timeout&&(s.timeout=e.timeout),e.responseType){let E=e.responseType.toLowerCase();s.responseType=E!=="json"?E:"text"}let a=e.serializeBody(),l=null,c=()=>{if(l!==null)return l;let E=s.statusText||"OK",I=new ji(s.getAllResponseHeaders()),ce=s.responseURL||e.url;return l=new Y_({headers:I,status:s.status,statusText:E,url:ce}),l},u=this.maybePropagateTrace(()=>{let{headers:E,status:I,statusText:ce,url:nt}=c(),Xe=null;I!==aV&&(Xe=typeof s.response>"u"?s.responseText:s.response),I===0&&(I=Xe?sV:0);let Qi=I>=200&&I<300;if(e.responseType==="json"&&typeof Xe=="string"){let wi=Xe;Xe=Xe.replace(lV,"");try{Xe=Xe!==""?JSON.parse(Xe):null}catch(Gr){Xe=wi,Qi&&(Qi=!1,Xe={error:Gr,text:Xe})}}Qi?(o.next(new Hs({body:Xe,headers:E,status:I,statusText:ce,url:nt||void 0})),o.complete()):o.error(new Us({error:Xe,headers:E,status:I,statusText:ce,url:nt||void 0}))}),f=this.maybePropagateTrace(E=>{let{url:I}=c(),ce=new Us({error:E,status:s.status||0,statusText:s.statusText||"Unknown Error",url:I||void 0});o.error(ce)}),h=f;e.timeout&&(h=this.maybePropagateTrace(E=>{let{url:I}=c(),ce=new Us({error:new DOMException("Request timed out","TimeoutError"),status:s.status||0,statusText:s.statusText||"Request timeout",url:I||void 0});o.error(ce)}));let m=!1,p=this.maybePropagateTrace(E=>{m||(o.next(c()),m=!0);let I={type:So.DownloadProgress,loaded:E.loaded};E.lengthComputable&&(I.total=E.total),e.responseType==="text"&&s.responseText&&(I.partialText=s.responseText),o.next(I)}),w=this.maybePropagateTrace(E=>{let I={type:So.UploadProgress,loaded:E.loaded};E.lengthComputable&&(I.total=E.total),o.next(I)});return s.addEventListener("load",u),s.addEventListener("error",f),s.addEventListener("timeout",h),s.addEventListener("abort",f),e.reportProgress&&(s.addEventListener("progress",p),a!==null&&s.upload&&s.upload.addEventListener("progress",w)),s.send(a),o.next({type:So.Sent}),()=>{s.removeEventListener("error",f),s.removeEventListener("abort",f),s.removeEventListener("load",u),s.removeEventListener("timeout",h),e.reportProgress&&(s.removeEventListener("progress",p),a!==null&&s.upload&&s.upload.removeEventListener("progress",w)),s.readyState!==s.DONE&&s.abort()}})))}static \u0275fac=function(i){return new(i||t)(K(xo))};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function dV(t,n){return n(t)}function uV(t,n,e){return(i,r)=>wt(e,()=>n(i,o=>t(o,r)))}var fV=new g("",{factory:()=>[]}),Z_=new g(""),hV=new g("",{factory:()=>!0});var mV=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=K(cV),r},providedIn:"root"})}return t})();var pV=(()=>{class t{backend;injector;chain=null;pendingTasks=d(ys);contributeToStability=d(hV);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(fV),...this.injector.get(Z_,[])]));this.chain=i.reduceRight((r,o)=>uV(r,o,this.injector),dV)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(io(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||t)(K(mV),K(Ne))};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),gV=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=K(pV),r},providedIn:"root"})}return t})();function G_(t,n){return{body:n,headers:t.headers,context:t.context,observe:t.observe,params:t.params,reportProgress:t.reportProgress,responseType:t.responseType,withCredentials:t.withCredentials,credentials:t.credentials,transferCache:t.transferCache,timeout:t.timeout,keepalive:t.keepalive,priority:t.priority,cache:t.cache,mode:t.mode,redirect:t.redirect,integrity:t.integrity,referrer:t.referrer,referrerPolicy:t.referrerPolicy}}var K_=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof Bs)o=e;else{let l;r.headers instanceof ji?l=r.headers:l=new ji(r.headers);let c;r.params&&(r.params instanceof Vi?c=r.params:c=new Vi({fromObject:r.params})),o=new Bs(e,i,r.body!==void 0?r.body:null,{headers:l,context:r.context,params:c,reportProgress:r.reportProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=B(o).pipe(no(l=>this.handler.handle(l)));if(e instanceof Bs||r.observe==="events")return s;let a=s.pipe(be(l=>l instanceof Hs));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(fe(l=>{if(l.body!==null&&!(l.body instanceof ArrayBuffer))throw new S(2806,!1);return l.body}));case"blob":return a.pipe(fe(l=>{if(l.body!==null&&!(l.body instanceof Blob))throw new S(2807,!1);return l.body}));case"text":return a.pipe(fe(l=>{if(l.body!==null&&typeof l.body!="string")throw new S(2808,!1);return l.body}));default:return a.pipe(fe(l=>l.body))}case"response":return a;default:throw new S(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new Vi().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,G_(r,i))}post(e,i,r={}){return this.request("POST",e,G_(r,i))}put(e,i,r={}){return this.request("PUT",e,G_(r,i))}static \u0275fac=function(i){return new(i||t)(K(gV))};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var vV=new g(""),_V="b",yV="h",bV="s",DV="st",wV="u",CV="rt",Q_=new g(""),EV=["GET","HEAD"];function YS(t,n){let s=n,{isCacheActive:e}=s,i=iD(s,["isCacheActive"]),{transferCache:r,method:o}=t;return!(!e||r===!1||t.withCredentials||o==="POST"&&!i.includePostRequests&&!r||o!=="POST"&&!EV.includes(o)||!i.includeRequestsWithAuthHeaders&&IV(t)||i.filter?.(t)===!1)}function ZS(t,n){let{includeHeaders:e}=t,i=e;return typeof n=="object"&&n.includeHeaders&&(i=n.includeHeaders),i}function xV(t,n,e,i){let{transferCache:r}=t;if(!YS(t,n))return null;if(i)throw new S(2803,!1);let o=t.url,s=KS(t,o),a=e.get(s,null),l=ZS(n,r);if(a){let{[_V]:c,[CV]:u,[yV]:f,[bV]:h,[DV]:m,[wV]:p}=a,w=c;switch(u){case"arraybuffer":w=qS(c);break;case"blob":w=new Blob([qS(c)]);break}let E=new ji(f);return new Hs({body:w,headers:E,status:h,statusText:m,url:p})}return null}function SV(t,n){let e=d(Q_),i=d(Ts),r=d(vV,{optional:!0}),o=xV(t,e,i,r);if(o)return B(o);let{transferCache:s}=t,a=ZS(e,s),l=t.url,c=KS(t,l);return YS(t,e),n(t)}function IV(t){return t.headers.has("authorization")||t.headers.has("proxy-authorization")||t.headers.has("cookie")}function WS(t){return[...t.keys()].sort().map(n=>`${n}=${t.getAll(n)}`).join("&")}function KS(t,n){let{params:e,method:i,responseType:r}=t,o=WS(e),s=t.serializeBody();s instanceof URLSearchParams?s=WS(s):typeof s!="string"&&(s="");let a=[i,r,n,s,o].join("|"),l=MV(a);return l}function MV(t){let n=0;for(let e of t)n=Math.imul(31,n)+e.charCodeAt(0)<<0;return n+=2147483648,n.toString()}function qS(t){let n=atob(t);return Uint8Array.from(n,i=>i.charCodeAt(0)).buffer}function QS(t){return[{provide:Q_,useFactory:()=>(yn("NgHttpTransferCache"),y({isCacheActive:!0},t))},{provide:Z_,useValue:SV,multi:!0},{provide:yr,multi:!0,useFactory:()=>{let n=d(ht),e=d(Q_);return()=>{n.whenStable().then(()=>{e.isCacheActive=!1})}}}]}var XS=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(K(P))};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var ql=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=K(AV),r},providedIn:"root"})}return t})(),AV=(()=>{class t extends ql{_doc;constructor(e){super(),this._doc=e}sanitize(e,i){if(i==null)return null;switch(e){case _t.NONE:return i;case _t.HTML:return Ri(i,"HTML")?Ln(i):Ev(this._doc,String(i)).toString();case _t.STYLE:return Ri(i,"Style")?Ln(i):i;case _t.SCRIPT:if(Ri(i,"Script"))return Ln(i);throw new S(5200,!1);case _t.URL:return Ri(i,"URL")?Ln(i):Ml(String(i));case _t.RESOURCE_URL:if(Ri(i,"ResourceURL"))return Ln(i);throw new S(5201,!1);default:throw new S(5202,!1)}}bypassSecurityTrustHtml(e){return yv(e)}bypassSecurityTrustStyle(e){return bv(e)}bypassSecurityTrustScript(e){return Dv(e)}bypassSecurityTrustUrl(e){return wv(e)}bypassSecurityTrustResourceUrl(e){return Cv(e)}static \u0275fac=function(i){return new(i||t)(K(P))};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Uf=(function(t){return t[t.NoHttpTransferCache=0]="NoHttpTransferCache",t[t.HttpTransferCacheOptions=1]="HttpTransferCacheOptions",t[t.I18nSupport=2]="I18nSupport",t[t.EventReplay=3]="EventReplay",t[t.IncrementalHydration=4]="IncrementalHydration",t})(Uf||{});function RV(t,n=[],e={}){return{\u0275kind:t,\u0275providers:n}}function JS(){return RV(Uf.EventReplay,hS())}function eI(...t){let n=[],e=new Set;for(let{\u0275providers:r,\u0275kind:o}of t)e.add(o),r.length&&n.push(r);let i=e.has(Uf.HttpTransferCacheOptions);return ei([[],[],mS(),e.has(Uf.NoHttpTransferCache)||i?[]:QS({}),n])}var pe="primary",lc=Symbol("RouteTitle"),ny=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function Mo(t){return new ny(t)}function X_(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function lI(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let l={},c=t.slice(0,i.length);return X_(i,c,l)?{consumed:c,posParams:l}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let a={};return!X_(o,t.slice(0,o.length),a)||!X_(s,t.slice(t.length-s.length),a)?null:{consumed:t,posParams:a}}function qf(t){return new Promise((n,e)=>{t.pipe(xi()).subscribe({next:i=>n(i),error:i=>e(i)})})}function kV(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!hi(t[e],n[e]))return!1;return!0}function hi(t,n){let e=t?iy(t):void 0,i=n?iy(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!cI(t[r],n[r]))return!1;return!0}function iy(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function cI(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function OV(t){return t.length>0?t[t.length-1]:null}function Ro(t){return eo(t)?t:_r(t)?He(Promise.resolve(t)):B(t)}function dI(t){return eo(t)?qf(t):Promise.resolve(t)}var NV={exact:fI,subset:hI},uI={exact:PV,subset:FV,ignored:()=>!0},vy={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},Jl={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function _y(t,n,e){let i=t instanceof Wt?t:n.parseUrl(t);return Pt(()=>ry(n.lastSuccessfulNavigation()?.finalUrl??new Wt,i,y(y({},Jl),e)))}function ry(t,n,e){return NV[e.paths](t.root,n.root,e.matrixParams)&&uI[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function PV(t,n){return hi(t,n)}function fI(t,n,e){if(!Io(t.segments,n.segments)||!$f(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!fI(t.children[i],n.children[i],e))return!1;return!0}function FV(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>cI(t[e],n[e]))}function hI(t,n,e){return mI(t,n,n.segments,e)}function mI(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!Io(r,e)||n.hasChildren()||!$f(r,e,i))}else if(t.segments.length===e.length){if(!Io(t.segments,e)||!$f(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!hI(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!Io(t.segments,r)||!$f(t.segments,r,i)||!t.children[pe]?!1:mI(t.children[pe],n,o,i)}}function $f(t,n,e){return n.every((i,r)=>uI[e](t[r].parameters,i.parameters))}var Wt=class{root;queryParams;fragment;_queryParamMap;constructor(n=new ke([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=Mo(this.queryParams),this._queryParamMap}toString(){return jV.serialize(this)}},ke=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return Gf(this)}},Sr=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=Mo(this.parameters),this._parameterMap}toString(){return gI(this)}};function LV(t,n){return Io(t,n)&&t.every((e,i)=>hi(e.parameters,n[i].parameters))}function Io(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function VV(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===pe&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==pe&&(e=e.concat(n(r,i)))}),e}var Qs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:()=>new Ir,providedIn:"root"})}return t})(),Ir=class{parse(n){let e=new sy(n);return new Wt(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${Yl(n.root,!0)}`,i=HV(n.queryParams),r=typeof n.fragment=="string"?`#${BV(n.fragment)}`:"";return`${e}${i}${r}`}},jV=new Ir;function Gf(t){return t.segments.map(n=>gI(n)).join("/")}function Yl(t,n){if(!t.hasChildren())return Gf(t);if(n){let e=t.children[pe]?Yl(t.children[pe],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==pe&&i.push(`${r}:${Yl(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=VV(t,(i,r)=>r===pe?[Yl(t.children[pe],!1)]:[`${r}:${Yl(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[pe]!=null?`${Gf(t)}/${e[0]}`:`${Gf(t)}/(${e.join("//")})`}}function pI(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function Hf(t){return pI(t).replace(/%3B/gi,";")}function BV(t){return encodeURI(t)}function oy(t){return pI(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function Wf(t){return decodeURIComponent(t)}function nI(t){return Wf(t.replace(/\+/g,"%20"))}function gI(t){return`${oy(t.path)}${UV(t.parameters)}`}function UV(t){return Object.entries(t).map(([n,e])=>`;${oy(n)}=${oy(e)}`).join("")}function HV(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${Hf(e)}=${Hf(r)}`).join("&"):`${Hf(e)}=${Hf(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var zV=/^[^\/()?;#]+/;function J_(t){let n=t.match(zV);return n?n[0]:""}var $V=/^[^\/()?;=#]+/;function GV(t){let n=t.match($V);return n?n[0]:""}var WV=/^[^=?&#]+/;function qV(t){let n=t.match(WV);return n?n[0]:""}var YV=/^[^&#]+/;function ZV(t){let n=t.match(YV);return n?n[0]:""}var sy=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new ke([],{}):new ke([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new S(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[pe]=new ke(e,i)),r}parseSegment(){let n=J_(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new S(4009,!1);return this.capture(n),new Sr(Wf(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=GV(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=J_(this.remaining);r&&(i=r,this.capture(i))}n[Wf(e)]=Wf(i)}parseQueryParam(n){let e=qV(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let s=ZV(this.remaining);s&&(i=s,this.capture(i))}let r=nI(e),o=nI(i);if(n.hasOwnProperty(r)){let s=n[r];Array.isArray(s)||(s=[s],n[r]=s),s.push(o)}else n[r]=o}parseParens(n,e){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=J_(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new S(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):n&&(s=pe);let a=this.parseChildren(e+1);i[s??pe]=Object.keys(a).length===1&&a[pe]?a[pe]:new ke([],a),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new S(4011,!1)}};function vI(t){return t.segments.length>0?new ke([],{[pe]:t}):t}function _I(t){let n={};for(let[i,r]of Object.entries(t.children)){let o=_I(r);if(i===pe&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))n[s]=a;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new ke(t.segments,n);return KV(e)}function KV(t){if(t.numberOfChildren===1&&t.children[pe]){let n=t.children[pe];return new ke(t.segments.concat(n.segments),n.children)}return t}function Mr(t){return t instanceof Wt}function yI(t,n,e=null,i=null,r=new Ir){let o=bI(t);return DI(o,n,e,i,r)}function bI(t){let n;function e(o){let s={};for(let l of o.children){let c=e(l);s[l.outlet]=c}let a=new ke(o.url,s);return o===t&&(n=a),a}let i=e(t.root),r=vI(i);return n??r}function DI(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return ey(o,o,o,e,i,r);let s=QV(n);if(s.toRoot())return ey(o,o,new ke([],{}),e,i,r);let a=XV(s,o,t),l=a.processChildren?Kl(a.segmentGroup,a.index,s.commands):CI(a.segmentGroup,a.index,s.commands);return ey(o,a.segmentGroup,l,e,i,r)}function Yf(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function ec(t){return typeof t=="object"&&t!=null&&t.outlets}function iI(t,n,e){t||="\u0275";let i=new Wt;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function ey(t,n,e,i,r,o){let s={};for(let[c,u]of Object.entries(i??{}))s[c]=Array.isArray(u)?u.map(f=>iI(c,f,o)):iI(c,u,o);let a;t===n?a=e:a=wI(t,n,e);let l=vI(_I(a));return new Wt(l,s,r)}function wI(t,n,e){let i={};return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=wI(o,n,e)}),new ke(t.segments,i)}var Zf=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&Yf(i[0]))throw new S(4003,!1);let r=i.find(ec);if(r&&r!==OV(i))throw new S(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function QV(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new Zf(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([l,c])=>{a[l]=typeof c=="string"?c.split("/"):c}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,l)=>{l==0&&a==="."||(l==0&&a===""?e=!0:a===".."?n++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new Zf(e,n,i)}var $s=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function XV(t,n,e){if(t.isAbsolute)return new $s(n,!0,0);if(!e)return new $s(n,!1,NaN);if(e.parent===null)return new $s(e,!0,0);let i=Yf(t.commands[0])?0:1,r=e.segments.length-1+i;return JV(e,r,t.numberOfDoubleDots)}function JV(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new S(4005,!1);r=i.segments.length}return new $s(i,!1,r-o)}function ej(t){return ec(t[0])?t[0].outlets:{[pe]:t}}function CI(t,n,e){if(t??=new ke([],{}),t.segments.length===0&&t.hasChildren())return Kl(t,n,e);let i=tj(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new ke(t.segments.slice(0,i.pathIndex),{});return o.children[pe]=new ke(t.segments.slice(i.pathIndex),t.children),Kl(o,0,r)}else return i.match&&r.length===0?new ke(t.segments,{}):i.match&&!t.hasChildren()?ay(t,n,e):i.match?Kl(t,0,r):ay(t,n,e)}function Kl(t,n,e){if(e.length===0)return new ke(t.segments,{});{let i=ej(e),r={};if(Object.keys(i).some(o=>o!==pe)&&t.children[pe]&&t.numberOfChildren===1&&t.children[pe].segments.length===0){let o=Kl(t.children[pe],n,e);return new ke(t.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=CI(t.children[o],n,s))}),Object.entries(t.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new ke(t.segments,r)}}function tj(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let s=t.segments[r],a=e[i];if(ec(a))break;let l=`${a}`,c=i<e.length-1?e[i+1]:null;if(r>0&&l===void 0)break;if(l&&c&&typeof c=="object"&&c.outlets===void 0){if(!oI(l,c,s))return o;i+=2}else{if(!oI(l,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function ay(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(ec(o)){let l=nj(o.outlets);return new ke(i,l)}if(r===0&&Yf(e[0])){let l=t.segments[n];i.push(new Sr(l.path,rI(e[0]))),r++;continue}let s=ec(o)?o.outlets[pe]:`${o}`,a=r<e.length-1?e[r+1]:null;s&&a&&Yf(a)?(i.push(new Sr(s,rI(a))),r+=2):(i.push(new Sr(s,{})),r++)}return new ke(i,{})}function nj(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=ay(new ke([],{}),0,i))}),n}function rI(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function oI(t,n,e){return t==e.path&&hi(n,e.parameters)}var Ql="imperative",Et=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(Et||{}),ln=class{id;url;constructor(n,e){this.id=n,this.url=e}},To=class extends ln{type=Et.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},Bn=class extends ln{urlAfterRedirects;type=Et.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},Ft=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(Ft||{}),tc=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(tc||{}),Dn=class extends ln{reason;code;type=Et.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function EI(t){return t instanceof Dn&&(t.code===Ft.Redirect||t.code===Ft.SupersededByNewNavigation)}var Ui=class extends ln{reason;code;type=Et.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},Ao=class extends ln{error;target;type=Et.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},nc=class extends ln{urlAfterRedirects;state;type=Et.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Kf=class extends ln{urlAfterRedirects;state;type=Et.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Qf=class extends ln{urlAfterRedirects;state;shouldActivate;type=Et.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Xf=class extends ln{urlAfterRedirects;state;type=Et.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Jf=class extends ln{urlAfterRedirects;state;type=Et.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},eh=class{route;type=Et.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},th=class{route;type=Et.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},nh=class{snapshot;type=Et.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},ih=class{snapshot;type=Et.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},rh=class{snapshot;type=Et.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},oh=class{snapshot;type=Et.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var Ws=class{},ic=class{},qs=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function ij(t){return!(t instanceof Ws)&&!(t instanceof qs)&&!(t instanceof ic)}var sh=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new Xs(this.rootInjector)}},Xs=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new sh(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(K(Ne))};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),ah=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=ly(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=ly(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=cy(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return cy(n,this._root).map(e=>e.value)}};function ly(t,n){if(t===n.value)return n;for(let e of n.children){let i=ly(t,e);if(i)return i}return null}function cy(t,n){if(t===n.value)return[n];for(let e of n.children){let i=cy(t,e);if(i.length)return i.unshift(n),i}return[]}var an=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function zs(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var rc=class extends ah{snapshot;constructor(n,e){super(n),this.snapshot=e,by(this,n)}toString(){return this.snapshot.toString()}};function xI(t,n){let e=rj(t,n),i=new rt([new Sr("",{})]),r=new rt({}),o=new rt({}),s=new rt({}),a=new rt(""),l=new Hi(i,r,s,a,o,pe,t,e.root);return l.snapshot=e.root,new rc(new an(l,[]),e)}function rj(t,n){let e={},i={},r={},s=new Ys([],e,r,"",i,pe,t,null,{},n);return new oc("",new an(s,[]))}var Hi=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(n,e,i,r,o,s,a,l){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=l,this.title=this.dataSubject?.pipe(fe(c=>c[lc]))??B(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(fe(n=>Mo(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(fe(n=>Mo(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function yy(t,n,e="emptyOnly"){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:y(y({},n.params),t.params),data:y(y({},n.data),t.data),resolve:y(y(y(y({},t.data),n.data),r?.data),t._resolvedData)}:i={params:y({},t.params),data:y({},t.data),resolve:y(y({},t.data),t._resolvedData??{})},r&&II(r)&&(i.resolve[lc]=r.title),i}var Ys=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[lc]}constructor(n,e,i,r,o,s,a,l,c,u){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=l,this._resolve=c,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=Mo(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=Mo(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},oc=class extends ah{url;constructor(n,e){super(e),this.url=n,by(this,e)}toString(){return SI(this._root)}};function by(t,n){n.value._routerState=t,n.children.forEach(e=>by(t,e))}function SI(t){let n=t.children.length>0?` { ${t.children.map(SI).join(", ")} } `:"";return`${t.value}${n}`}function ty(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,hi(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),hi(n.params,e.params)||t.paramsSubject.next(e.params),kV(n.url,e.url)||t.urlSubject.next(e.url),hi(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function dy(t,n){let e=hi(t.params,n.params)&&LV(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||dy(t.parent,n.parent))}function II(t){return typeof t.title=="string"||t.title===null}var MI=new g(""),ko=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=pe;activateEvents=new j;deactivateEvents=new j;attachEvents=new j;detachEvents=new j;routerOutletData=uS();parentContexts=d(Xs);location=d(Qe);changeDetector=d(Pe);inputBinder=d(cc,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new S(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new S(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new S(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new S(4013,!1);this._activatedRoute=e;let r=this.location,s=e.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,l=new uy(e,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:l,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[Ie]})}return t})(),uy=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===Hi?this.route:n===Xs?this.childContexts:n===MI?this.outletData:this.parent.get(n,e)}},cc=new g(""),Dy=(()=>{class t{outletDataSubscriptions=new Map;bindActivatedRouteToOutletComponent(e){this.unsubscribeFromRouteData(e),this.subscribeToRouteData(e)}unsubscribeFromRouteData(e){this.outletDataSubscriptions.get(e)?.unsubscribe(),this.outletDataSubscriptions.delete(e)}subscribeToRouteData(e){let{activatedRoute:i}=e,r=Kn([i.queryParams,i.params,i.data]).pipe(ot(([o,s,a],l)=>(a=y(y(y({},o),s),a),l===0?B(a):Promise.resolve(a)))).subscribe(o=>{if(!e.isActivated||!e.activatedComponentRef||e.activatedRoute!==i||i.component===null){this.unsubscribeFromRouteData(e);return}let s=_S(i.component);if(!s){this.unsubscribeFromRouteData(e);return}for(let{templateName:a}of s.inputs)e.activatedComponentRef.setInput(a,o[a])});this.outletDataSubscriptions.set(e,r)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac})}return t})(),wy=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&he(0,"router-outlet")},dependencies:[ko],encapsulation:2})}return t})();function Cy(t){let n=t.children&&t.children.map(Cy),e=n?te(y({},t),{children:n}):y({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==pe&&(e.component=wy),e}function oj(t,n,e){let i=sc(t,n._root,e?e._root:void 0);return new rc(i,n)}function sc(t,n,e){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let i=e.value;i._futureSnapshot=n.value;let r=sj(t,n,e);return new an(i,r)}else{if(t.shouldAttach(n.value)){let o=t.retrieve(n.value);if(o!==null){let s=o.route;return s.value._futureSnapshot=n.value,s.children=n.children.map(a=>sc(t,a)),s}}let i=aj(n.value),r=n.children.map(o=>sc(t,o));return new an(i,r)}}function sj(t,n,e){return n.children.map(i=>{for(let r of e.children)if(t.shouldReuseRoute(i.value,r.value.snapshot))return sc(t,i,r);return sc(t,i)})}function aj(t){return new Hi(new rt(t.url),new rt(t.params),new rt(t.queryParams),new rt(t.fragment),new rt(t.data),t.outlet,t.component,t)}var Zs=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},TI="ngNavigationCancelingError";function lh(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=Mr(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=AI(!1,Ft.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function AI(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[TI]=!0,e.cancellationCode=n,e}function lj(t){return RI(t)&&Mr(t.url)}function RI(t){return!!t&&t[TI]}var fy=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),ty(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=zs(e);n.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(n,e,s.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=zs(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:s,route:n,contexts:a})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=zs(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(n,e,i){let r=zs(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new oh(o.value.snapshot))}),n.children.length&&this.forwardEvent(new ih(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(ty(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,s.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),ty(a.route.value),this.activateChildRoutes(n,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(n,null,s.children)}else this.activateChildRoutes(n,null,i)}},ch=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},Gs=class{component;route;constructor(n,e){this.component=n,this.route=e}};function cj(t,n,e){let i=t._root,r=n?n._root:null;return Zl(i,r,e,[i.value])}function dj(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function Js(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!_p(t)?t:n.get(t):i}function Zl(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=zs(n);return t.children.forEach(s=>{uj(s,o[s.value.outlet],e,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>Xl(a,e.getContext(s),r)),r}function uj(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,s=n?n.value:null,a=e?e.getContext(t.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let l=fj(s,o,o.routeConfig.runGuardsAndResolvers);l?r.canActivateChecks.push(new ch(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?Zl(t,n,a?a.children:null,i,r):Zl(t,n,e,i,r),l&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new Gs(a.outlet.component,s))}else s&&Xl(n,a,r),r.canActivateChecks.push(new ch(i)),o.component?Zl(t,null,a?a.children:null,i,r):Zl(t,null,e,i,r);return r}function fj(t,n,e){if(typeof e=="function")return wt(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!Io(t.url,n.url);case"pathParamsOrQueryParamsChange":return!Io(t.url,n.url)||!hi(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!dy(t,n)||!hi(t.queryParams,n.queryParams);default:return!dy(t,n)}}function Xl(t,n,e){let i=zs(t),r=t.value;Object.entries(i).forEach(([o,s])=>{r.component?n?Xl(s,n.children.getContext(o),e):Xl(s,null,e):Xl(s,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new Gs(n.outlet.component,r)):e.canDeactivateChecks.push(new Gs(null,r)):e.canDeactivateChecks.push(new Gs(null,r))}function dc(t){return typeof t=="function"}function hj(t){return typeof t=="boolean"}function mj(t){return t&&dc(t.canLoad)}function pj(t){return t&&dc(t.canActivate)}function gj(t){return t&&dc(t.canActivateChild)}function vj(t){return t&&dc(t.canDeactivate)}function _j(t){return t&&dc(t.canMatch)}function kI(t){return t instanceof to||t?.name==="EmptyError"}var zf=Symbol("INITIAL_VALUE");function Ks(){return ot(t=>Kn(t.map(n=>n.pipe(ze(1),Je(zf)))).pipe(fe(n=>{for(let e of n)if(e!==!0){if(e===zf)return zf;if(e===!1||yj(e))return e}return!0}),be(n=>n!==zf),ze(1)))}function yj(t){return Mr(t)||t instanceof Zs}function OI(t){return t.aborted?B(void 0).pipe(ze(1)):new de(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function NI(t){return Ae(OI(t))}function bj(t){return Rt(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?B(te(y({},n),{guardsResult:!0})):Dj(o,e,i).pipe(Rt(s=>s&&hj(s)?wj(e,r,t):B(s)),fe(s=>te(y({},n),{guardsResult:s})))})}function Dj(t,n,e){return He(t).pipe(Rt(i=>Ij(i.component,i.route,e,n)),xi(i=>i!==!0,!0))}function wj(t,n,e){return He(n).pipe(no(i=>sr(Ej(i.route.parent,e),Cj(i.route,e),Sj(t,i.path),xj(t,i.route))),xi(i=>i!==!0,!0))}function Cj(t,n){return t!==null&&n&&n(new rh(t)),B(!0)}function Ej(t,n){return t!==null&&n&&n(new nh(t)),B(!0)}function xj(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return B(!0);let i=e.map(r=>xn(()=>{let o=n._environmentInjector,s=Js(r,o),a=pj(s)?s.canActivate(n,t):wt(o,()=>s(n,t));return Ro(a).pipe(xi())}));return B(i).pipe(Ks())}function Sj(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>dj(o)).filter(o=>o!==null).map(o=>xn(()=>{let s=o.guards.map(a=>{let l=o.node._environmentInjector,c=Js(a,l),u=gj(c)?c.canActivateChild(e,t):wt(l,()=>c(e,t));return Ro(u).pipe(xi())});return B(s).pipe(Ks())}));return B(r).pipe(Ks())}function Ij(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return B(!0);let o=r.map(s=>{let a=n._environmentInjector,l=Js(s,a),c=vj(l)?l.canDeactivate(t,n,e,i):wt(a,()=>l(t,n,e,i));return Ro(c).pipe(xi())});return B(o).pipe(Ks())}function Mj(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return B(!0);let s=o.map(a=>{let l=Js(a,t),c=mj(l)?l.canLoad(n,e):wt(t,()=>l(n,e)),u=Ro(c);return r?u.pipe(NI(r)):u});return B(s).pipe(Ks(),PI(i))}function PI(t){return Ym(Dt(n=>{if(typeof n!="boolean")throw lh(t,n)}),fe(n=>n===!0))}function Tj(t,n,e,i,r,o){let s=n.canMatch;if(!s||s.length===0)return B(!0);let a=s.map(l=>{let c=Js(l,t),u=_j(c)?c.canMatch(n,e,r):wt(t,()=>c(n,e,r));return Ro(u).pipe(NI(o))});return B(a).pipe(Ks(),PI(i))}var Bi=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},ac=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function Aj(t){throw new S(4e3,!1)}function Rj(t){throw AI(!1,Ft.GuardRejected)}var hy=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[pe])throw Aj(`${n.redirectTo}`);r=r.children[pe]}}async applyRedirectCommands(n,e,i,r,o){let s=await kj(e,r,o);if(s instanceof Wt)throw new ac(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),n,i);if(s[0]==="/")throw new ac(a);return a}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new Wt(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=e[a]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),s={};return Object.entries(e.children).forEach(([a,l])=>{s[a]=this.createSegmentGroup(n,l,i,r)}),new ke(o,s)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new S(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function kj(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return qf(Ro(wt(e,()=>i(n))))}function Oj(t,n){return t.providers&&!t._injector&&(t._injector=Ns(t.providers,n,`Route: ${t.path}`)),t._injector??n}function jn(t){return t.outlet||pe}function Nj(t,n){let e=t.filter(i=>jn(i)===n);return e.push(...t.filter(i=>jn(i)!==n)),e}var my={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function FI(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function Pj(t,n,e,i,r,o,s){let a=LI(t,n,e);if(!a.matched)return B(a);let l=FI(o(a));return i=Oj(n,i),Tj(i,n,e,r,l,s).pipe(fe(c=>c===!0?a:y({},my)))}function LI(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?y({},my):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||lI)(e,t,n);if(!r)return y({},my);let o={};Object.entries(r.posParams??{}).forEach(([a,l])=>{o[a]=l.path});let s=r.consumed.length>0?y(y({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function sI(t,n,e,i,r){return e.length>0&&Vj(t,e,i,r)?{segmentGroup:new ke(n,Lj(i,new ke(e,t.children))),slicedSegments:[]}:e.length===0&&jj(t,e,i)?{segmentGroup:new ke(t.segments,Fj(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new ke(t.segments,t.children),slicedSegments:e}}function Fj(t,n,e,i){let r={};for(let o of e)if(uh(t,n,o)&&!i[jn(o)]){let s=new ke([],{});r[jn(o)]=s}return y(y({},i),r)}function Lj(t,n){let e={};e[pe]=n;for(let i of t)if(i.path===""&&jn(i)!==pe){let r=new ke([],{});e[jn(i)]=r}return e}function Vj(t,n,e,i){return e.some(r=>!uh(t,n,r)||!(jn(r)!==pe)?!1:!(i!==void 0&&jn(r)===i))}function jj(t,n,e){return e.some(i=>uh(t,n,i))}function uh(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function Bj(t,n,e){return n.length===0&&!t.children[e]}var py=class{};async function Uj(t,n,e,i,r,o,s="emptyOnly",a){return new gy(t,n,e,i,r,s,o,a).recognize()}var Hj=31,gy=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,s,a,l){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=l,this.applyRedirects=new hy(this.urlSerializer,this.urlTree)}noMatchError(n){return new S(4002,`'${n.segmentGroup}'`)}async recognize(){let n=sI(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=await this.match(n),r=new an(i,e),o=new oc("",r),s=yI(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}}async match(n){let e=new Ys([],Object.freeze({}),Object.freeze(y({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),pe,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,pe,e),rootSnapshot:e}}catch(i){if(i instanceof ac)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof Bi?this.noMatchError(i):i}}async processSegmentGroup(n,e,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let s=await this.processSegment(n,e,i,i.segments,r,!0,o);return s instanceof an?[s]:[]}async processChildren(n,e,i,r){let o=[];for(let l of Object.keys(i.children))l==="primary"?o.unshift(l):o.push(l);let s=[];for(let l of o){let c=i.children[l],u=Nj(e,l),f=await this.processSegmentGroup(n,u,c,l,r);s.push(...f)}let a=VI(s);return zj(a),a}async processSegment(n,e,i,r,o,s,a){for(let l of e)try{return await this.processSegmentAgainstRoute(l._injector??n,e,l,i,r,o,s,a)}catch(c){if(c instanceof Bi||kI(c))continue;throw c}if(Bj(i,r,o))return new py;throw new Bi(i)}async processSegmentAgainstRoute(n,e,i,r,o,s,a,l){if(jn(i)!==s&&(s===pe||!uh(r,o,i)))throw new Bi(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,s,l);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,s,l);throw new Bi(r)}async expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,s,a){let{matched:l,parameters:c,consumedSegments:u,positionalParamSegments:f,remainingSegments:h}=LI(e,r,o);if(!l)throw new Bi(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>Hj&&(this.allowRedirects=!1));let m=this.createSnapshot(n,r,o,c,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let p=await this.applyRedirects.applyRedirectCommands(u,r.redirectTo,f,FI(m),n),w=await this.applyRedirects.lineralizeSegments(r,p);return this.processSegment(n,i,e,w.concat(h),s,!1,a)}createSnapshot(n,e,i,r,o){let s=new Ys(i,r,Object.freeze(y({},this.urlTree.queryParams)),this.urlTree.fragment,Gj(e),jn(e),e.component??e._loadedComponent??null,e,Wj(e),n),a=yy(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}async matchSegmentAgainstRoute(n,e,i,r,o,s){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=nt=>this.createSnapshot(n,i,nt.consumedSegments,nt.parameters,s),l=await qf(Pj(e,i,r,n,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(e.children={}),!l?.matched)throw new Bi(e);n=i._injector??n;let{routes:c}=await this.getChildConfig(n,i,r),u=i._loadedInjector??n,{parameters:f,consumedSegments:h,remainingSegments:m}=l,p=this.createSnapshot(n,i,h,f,s),{segmentGroup:w,slicedSegments:E}=sI(e,h,m,c,o);if(E.length===0&&w.hasChildren()){let nt=await this.processChildren(u,c,w,p);return new an(p,nt)}if(c.length===0&&E.length===0)return new an(p,[]);let I=jn(i)===o,ce=await this.processSegment(u,c,w,E,I?pe:o,!0,p);return new an(p,ce instanceof an?[ce]:[])}async getChildConfig(n,e,i){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await qf(Mj(n,e,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw Rj(e)}return{routes:[],injector:n}}};function zj(t){t.sort((n,e)=>n.value.outlet===pe?-1:e.value.outlet===pe?1:n.value.outlet.localeCompare(e.value.outlet))}function $j(t){let n=t.value.routeConfig;return n&&n.path===""}function VI(t){let n=[],e=new Set;for(let i of t){if(!$j(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=VI(i.children);n.push(new an(i.value,r))}return n.filter(i=>!e.has(i))}function Gj(t){return t.data||{}}function Wj(t){return t.resolve||{}}function qj(t,n,e,i,r,o,s){return Rt(async a=>{let{state:l,tree:c}=await Uj(t,n,e,i,a.extractedUrl,r,o,s);return te(y({},a),{targetSnapshot:l,urlAfterRedirects:c})})}function Yj(t){return Rt(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return B(n);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let l of jI(a))o.add(l);let s=0;return He(o).pipe(no(a=>r.has(a)?Zj(a,e,t):(a.data=yy(a,a.parent,t).resolve,B(void 0))),Dt(()=>s++),Pd(1),Rt(a=>s===o.size?B(n):We))})}function jI(t){let n=t.children.map(e=>jI(e)).flat();return[t,...n]}function Zj(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!II(i)&&(r[lc]=i.title),xn(()=>(t.data=yy(t,t.parent,e).resolve,Kj(r,t,n).pipe(fe(o=>(t._resolvedData=o,t.data=y(y({},t.data),o),null)))))}function Kj(t,n,e){let i=iy(t);if(i.length===0)return B({});let r={};return He(i).pipe(Rt(o=>Qj(t[o],n,e).pipe(xi(),Dt(s=>{if(s instanceof Zs)throw lh(new Ir,s);r[o]=s}))),Pd(1),fe(()=>r),ar(o=>kI(o)?We:$a(o)))}function Qj(t,n,e){let i=n._environmentInjector,r=Js(t,i),o=r.resolve?r.resolve(n,e):wt(i,()=>r(n,e));return Ro(o)}function aI(t){return ot(n=>{let e=t(n);return e?He(e).pipe(fe(()=>n)):B(n)})}var Ey=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===pe);return i}getResolvedTitleForRoute(e){return e.data[lc]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:()=>d(BI),providedIn:"root"})}return t})(),BI=(()=>{class t extends Ey{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(K(XS))};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),ea=new g("",{factory:()=>({})}),uc=new g(""),UI=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=d(f_);async loadComponent(e,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await dI(wt(e,()=>i.loadComponent())),s=await $I(zI(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await HI(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();async function HI(t,n,e,i){let r=await dI(wt(e,()=>t.loadChildren())),o=await $I(zI(r)),s;o instanceof uf||Array.isArray(o)?s=o:s=await n.compileModuleAsync(o),i&&i(t);let a,l,c=!1,u;return Array.isArray(s)?(l=s,c=!0):(a=s.create(e).injector,u=s,l=a.get(uc,[],{optional:!0,self:!0}).flat()),{routes:l.map(Cy),injector:a,factory:u}}function Xj(t){return t&&typeof t=="object"&&"default"in t}function zI(t){return Xj(t)?t.default:t}async function $I(t){return t}var fh=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:()=>d(Jj),providedIn:"root"})}return t})(),Jj=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),GI=new g("");var eB=()=>{},WI=new g(""),qI=(()=>{class t{currentNavigation=k(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=k(null);events=new x;transitionAbortWithErrorSubject=new x;configLoader=d(UI);environmentInjector=d(Ne);destroyRef=d(zt);urlSerializer=d(Qs);rootContexts=d(Xs);location=d(xr);inputBindingEnabled=d(cc,{optional:!0})!==null;titleStrategy=d(Ey);options=d(ea,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=d(fh);createViewTransition=d(GI,{optional:!0});navigationErrorHandler=d(WI,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>B(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new eh(r)),i=r=>this.events.next(new th(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;Te(()=>{this.transitions?.next(te(y({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new rt(null),this.transitions.pipe(be(i=>i!==null),ot(i=>{let r=!1,o=new AbortController,s=()=>!r&&this.currentTransition?.id===i.id;return B(i).pipe(ot(a=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",Ft.SupersededByNewNavigation),We;this.currentTransition=i;let l=this.lastSuccessfulNavigation();this.currentNavigation.set({id:a.id,initialUrl:a.rawUrl,extractedUrl:a.extractedUrl,targetBrowserUrl:typeof a.extras.browserUrl=="string"?this.urlSerializer.parse(a.extras.browserUrl):a.extras.browserUrl,trigger:a.source,extras:a.extras,previousNavigation:l?te(y({},l),{previousNavigation:null}):null,abort:()=>o.abort(),routesRecognizeHandler:a.routesRecognizeHandler,beforeActivateHandler:a.beforeActivateHandler});let c=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=a.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!c&&u!=="reload")return this.events.next(new Ui(a.id,this.urlSerializer.serialize(a.rawUrl),"",tc.IgnoredSameUrlNavigation)),a.resolve(!1),We;if(this.urlHandlingStrategy.shouldProcessUrl(a.rawUrl))return B(a).pipe(ot(f=>(this.events.next(new To(f.id,this.urlSerializer.serialize(f.extractedUrl),f.source,f.restoredState)),f.id!==this.navigationId?We:Promise.resolve(f))),qj(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,o.signal),Dt(f=>{i.targetSnapshot=f.targetSnapshot,i.urlAfterRedirects=f.urlAfterRedirects,this.currentNavigation.update(h=>(h.finalUrl=f.urlAfterRedirects,h)),this.events.next(new ic)}),ot(f=>He(i.routesRecognizeHandler.deferredHandle??B(void 0)).pipe(fe(()=>f))),Dt(()=>{let f=new nc(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(f)}));if(c&&this.urlHandlingStrategy.shouldProcessUrl(a.currentRawUrl)){let{id:f,extractedUrl:h,source:m,restoredState:p,extras:w}=a,E=new To(f,this.urlSerializer.serialize(h),m,p);this.events.next(E);let I=xI(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=te(y({},a),{targetSnapshot:I,urlAfterRedirects:h,extras:te(y({},w),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(ce=>(ce.finalUrl=h,ce)),B(i)}else return this.events.next(new Ui(a.id,this.urlSerializer.serialize(a.extractedUrl),"",tc.IgnoredByUrlHandlingStrategy)),a.resolve(!1),We}),fe(a=>{let l=new Kf(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);return this.events.next(l),this.currentTransition=i=te(y({},a),{guards:cj(a.targetSnapshot,a.currentSnapshot,this.rootContexts)}),i}),bj(a=>this.events.next(a)),ot(a=>{if(i.guardsResult=a.guardsResult,a.guardsResult&&typeof a.guardsResult!="boolean")throw lh(this.urlSerializer,a.guardsResult);let l=new Qf(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot,!!a.guardsResult);if(this.events.next(l),!s())return We;if(!a.guardsResult)return this.cancelNavigationTransition(a,"",Ft.GuardRejected),We;if(a.guards.canActivateChecks.length===0)return B(a);let c=new Xf(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);if(this.events.next(c),!s())return We;let u=!1;return B(a).pipe(Yj(this.paramsInheritanceStrategy),Dt({next:()=>{u=!0;let f=new Jf(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(f)},complete:()=>{u||this.cancelNavigationTransition(a,"",Ft.NoDataFromResolver)}}))}),aI(a=>{let l=u=>{let f=[];if(u.routeConfig?._loadedComponent)u.component=u.routeConfig?._loadedComponent;else if(u.routeConfig?.loadComponent){let h=u._environmentInjector;f.push(this.configLoader.loadComponent(h,u.routeConfig).then(m=>{u.component=m}))}for(let h of u.children)f.push(...l(h));return f},c=l(a.targetSnapshot.root);return c.length===0?B(a):He(Promise.all(c).then(()=>a))}),aI(()=>this.afterPreactivation()),ot(()=>{let{currentSnapshot:a,targetSnapshot:l}=i,c=this.createViewTransition?.(this.environmentInjector,a.root,l.root);return c?He(c).pipe(fe(()=>i)):B(i)}),ze(1),ot(a=>{let l=oj(e.routeReuseStrategy,a.targetSnapshot,a.currentRouterState);this.currentTransition=i=a=te(y({},a),{targetRouterState:l}),this.currentNavigation.update(u=>(u.targetRouterState=l,u)),this.events.next(new Ws);let c=i.beforeActivateHandler.deferredHandle;return c?He(c.then(()=>a)):B(a)}),Dt(a=>{new fy(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),s()&&(r=!0,this.currentNavigation.update(l=>(l.abort=eB,l)),this.lastSuccessfulNavigation.set(Te(this.currentNavigation)),this.events.next(new Bn(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects))),this.titleStrategy?.updateTitle(a.targetRouterState.snapshot),a.resolve(!0))}),Ae(OI(o.signal).pipe(be(()=>!r&&!i.targetRouterState),Dt(()=>{this.cancelNavigationTransition(i,o.signal.reason+"",Ft.Aborted)}))),Dt({complete:()=>{r=!0}}),Ae(this.transitionAbortWithErrorSubject.pipe(Dt(a=>{throw a}))),io(()=>{o.abort(),r||this.cancelNavigationTransition(i,"",Ft.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),ar(a=>{if(r=!0,this.destroyed)return i.resolve(!1),We;if(RI(a))this.events.next(new Dn(i.id,this.urlSerializer.serialize(i.extractedUrl),a.message,a.cancellationCode)),lj(a)?this.events.next(new qs(a.url,a.navigationBehaviorOptions)):i.resolve(!1);else{let l=new Ao(i.id,this.urlSerializer.serialize(i.extractedUrl),a,i.targetSnapshot??void 0);try{let c=wt(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(c instanceof Zs){let{message:u,cancellationCode:f}=lh(this.urlSerializer,c);this.events.next(new Dn(i.id,this.urlSerializer.serialize(i.extractedUrl),u,f)),this.events.next(new qs(c.redirectTo,c.navigationBehaviorOptions))}else throw this.events.next(l),a}catch(c){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(c)}}return We}))}))}cancelNavigationTransition(e,i,r){let o=new Dn(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=Te(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function tB(t){return t!==Ql}var YI=new g("");var ZI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:()=>d(nB),providedIn:"root"})}return t})(),dh=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},nB=(()=>{class t extends dh{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),hh=(()=>{class t{urlSerializer=d(Qs);options=d(ea,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=d(xr);urlHandlingStrategy=d(fh);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new Wt;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,s=r??o;return s instanceof Wt?this.urlSerializer.serialize(s):s}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=xI(null,d(Ne));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:()=>d(iB),providedIn:"root"})}return t})(),iB=(()=>{class t extends hh{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof To?this.updateStateMemento():e instanceof Ui?this.commitTransition(i):e instanceof nc?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof Ws?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof Dn&&!EI(e)?this.restoreHistory(i):e instanceof Ao?this.restoreHistory(i,!0):e instanceof Bn&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:s,state:a}=r;if(this.location.isCurrentPathEqualTo(e)||s){let l=this.browserPageId,c=y(y({},a),this.generateNgRouterState(o,l,i));this.location.replaceState(e,"",c)}else{let l=y(y({},a),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",l)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?y({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):y({navigationId:e},this.routerUrlState(r))}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function xy(t,n){t.events.pipe(be(e=>e instanceof Bn||e instanceof Dn||e instanceof Ao||e instanceof Ui),fe(e=>e instanceof Bn||e instanceof Ui?0:(e instanceof Dn?e.code===Ft.Redirect||e.code===Ft.SupersededByNewNavigation:!1)?2:1),be(e=>e!==2),ze(1)).subscribe(()=>{n()})}var Un=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=d(hf);stateManager=d(hh);options=d(ea,{optional:!0})||{};pendingTasks=d(si);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=d(qI);urlSerializer=d(Qs);location=d(xr);urlHandlingStrategy=d(fh);injector=d(Ne);_events=new x;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=d(ZI);injectorCleanup=d(YI,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=d(uc,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!d(cc,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new ae;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=Te(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof Dn&&i.code!==Ft.Redirect&&i.code!==Ft.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof Bn)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof qs){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),l=y({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||tB(r.source)},s);this.scheduleNavigation(a,Ql,null,l,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}ij(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Ql,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let s=r?.navigationId?r:null,a=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=te(y({},o),{browserUrl:e})),r){let c=y({},r);delete c.navigationId,delete c.\u0275routerPageId,delete c.\u0275routerUrl,Object.keys(c).length!==0&&(o.state=c)}let l=this.parseUrl(a);this.scheduleNavigation(l,i,s,o).catch(c=>{this.disposed||this.injector.get(rn)(c)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return Te(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(Cy),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:l}=i,c=l?this.currentUrlTree.fragment:s,u=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":u=y(y({},this.currentUrlTree.queryParams),o);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=o||null}u!==null&&(u=this.removeEmptyProps(u));let f;try{let h=r?r.snapshot:this.routerState.snapshot.root;f=bI(h)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),f=this.currentUrlTree.root}return DI(f,e,u,c??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=Mr(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,Ql,null,i)}navigate(e,i={skipLocationChange:!1}){return rB(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(Xn(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=y({},vy):i===!1?r=y({},Jl):r=y(y({},Jl),i),Mr(e))return ry(this.currentUrlTree,e,r);let o=this.parseUrl(e);return ry(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,s){if(this.disposed)return Promise.resolve(!1);let a,l,c;s?(a=s.resolve,l=s.reject,c=s.promise):c=new Promise((f,h)=>{a=f,l=h});let u=this.pendingTasks.add();return xy(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:a,reject:l,promise:c,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),c.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function rB(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new S(4008,!1)}var sB=(()=>{class t{router=d(Un);stateManager=d(hh);fragment=k("");queryParams=k({});path=k("");serializer=d(Qs);constructor(){this.updateState(),this.router.events?.subscribe(e=>{e instanceof Bn&&this.updateState()})}updateState(){let{fragment:e,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(e),this.queryParams.set(r),this.path.set(this.serializer.serialize(new Wt(i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),ta=(()=>{class t{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=d(new Zt("href"),{optional:!0});reactiveHref=Df(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return Te(this.reactiveHref)}set href(e){this.reactiveHref.set(e)}set target(e){this._target.set(e)}get target(){return Te(this._target)}_target=k(void 0);set queryParams(e){this._queryParams.set(e)}get queryParams(){return Te(this._queryParams)}_queryParams=k(void 0,{equal:()=>!1});set fragment(e){this._fragment.set(e)}get fragment(){return Te(this._fragment)}_fragment=k(void 0);set queryParamsHandling(e){this._queryParamsHandling.set(e)}get queryParamsHandling(){return Te(this._queryParamsHandling)}_queryParamsHandling=k(void 0);set state(e){this._state.set(e)}get state(){return Te(this._state)}_state=k(void 0,{equal:()=>!1});set info(e){this._info.set(e)}get info(){return Te(this._info)}_info=k(void 0,{equal:()=>!1});set relativeTo(e){this._relativeTo.set(e)}get relativeTo(){return Te(this._relativeTo)}_relativeTo=k(void 0);set preserveFragment(e){this._preserveFragment.set(e)}get preserveFragment(){return Te(this._preserveFragment)}_preserveFragment=k(!1);set skipLocationChange(e){this._skipLocationChange.set(e)}get skipLocationChange(){return Te(this._skipLocationChange)}_skipLocationChange=k(!1);set replaceUrl(e){this._replaceUrl.set(e)}get replaceUrl(){return Te(this._replaceUrl)}_replaceUrl=k(!1);isAnchorElement;onChanges=new x;applicationErrorHandler=d(rn);options=d(ea,{optional:!0});reactiveRouterState=d(sB);constructor(e,i,r,o,s,a){this.router=e,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=s,this.locationStrategy=a;let l=s.nativeElement.tagName?.toLowerCase();this.isAnchorElement=l==="a"||l==="area"||!!(typeof customElements=="object"&&customElements.get(l)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(e){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",e)}ngOnChanges(e){this.onChanges.next(this)}routerLinkInput=k(null);set routerLink(e){e==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(Mr(e)?this.routerLinkInput.set(e):this.routerLinkInput.set(Array.isArray(e)?e:[e]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(e,i,r,o,s){let a=this._urlTree();if(a===null||this.isAnchorElement&&(e!==0||i||r||o||s||typeof this.target=="string"&&this.target!="_self"))return!0;let l={skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info};return this.router.navigateByUrl(a,l)?.catch(c=>{this.applicationErrorHandler(c)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(e,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,e,i):r.removeAttribute(o,e)}_urlTree=Pt(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let e=r=>r==="preserve"||r==="merge";(e(this._queryParamsHandling())||e(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:Mr(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(e,i)=>this.computeHref(e)===this.computeHref(i)});get urlTree(){return Te(this._urlTree)}computeHref(e){return e!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(e))??"":null}static \u0275fac=function(i){return new(i||t)(ne(Un),ne(Hi),El("tabindex"),ne(Be),ne(O),ne(Fs))};static \u0275dir=C({type:t,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&J("click",function(s){return r.onClick(s.button,s.ctrlKey,s.shiftKey,s.altKey,s.metaKey)}),i&2&&le("href",r.reactiveHref(),Iv)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",L],skipLocationChange:[2,"skipLocationChange","skipLocationChange",L],replaceUrl:[2,"replaceUrl","replaceUrl",L],routerLink:"routerLink"},features:[Ie]})}return t})(),Sy=(()=>{class t{router;element;renderer;cdr;links;classes=[];routerEventsSubscription;linkInputChangesSubscription;_isActive=!1;get isActive(){return this._isActive}routerLinkActiveOptions={exact:!1};ariaCurrentWhenActive;isActiveChange=new j;link=d(ta,{optional:!0});constructor(e,i,r,o){this.router=e,this.element=i,this.renderer=r,this.cdr=o,this.routerEventsSubscription=e.events.subscribe(s=>{s instanceof Bn&&this.update()})}ngAfterContentInit(){B(this.links.changes,B(null)).pipe(or()).subscribe(e=>{this.update(),this.subscribeToEachLinkOnChanges()})}subscribeToEachLinkOnChanges(){this.linkInputChangesSubscription?.unsubscribe();let e=[...this.links.toArray(),this.link].filter(i=>!!i).map(i=>i.onChanges);this.linkInputChangesSubscription=He(e).pipe(or()).subscribe(i=>{this._isActive!==this.isLinkActive(this.router)(i)&&this.update()})}set routerLinkActive(e){let i=Array.isArray(e)?e:e.split(" ");this.classes=i.filter(r=>!!r)}ngOnChanges(e){this.update()}ngOnDestroy(){this.routerEventsSubscription.unsubscribe(),this.linkInputChangesSubscription?.unsubscribe()}update(){!this.links||!this.router.navigated||queueMicrotask(()=>{let e=this.hasActiveLinks();this.classes.forEach(i=>{e?this.renderer.addClass(this.element.nativeElement,i):this.renderer.removeClass(this.element.nativeElement,i)}),e&&this.ariaCurrentWhenActive!==void 0?this.renderer.setAttribute(this.element.nativeElement,"aria-current",this.ariaCurrentWhenActive.toString()):this.renderer.removeAttribute(this.element.nativeElement,"aria-current"),this._isActive!==e&&(this._isActive=e,this.cdr.markForCheck(),this.isActiveChange.emit(e))})}isLinkActive(e){let i=aB(this.routerLinkActiveOptions)?this.routerLinkActiveOptions:this.routerLinkActiveOptions.exact??!1?y({},vy):y({},Jl);return r=>{let o=r.urlTree;return o?Te(_y(o,e,i)):!1}}hasActiveLinks(){let e=this.isLinkActive(this.router);return this.link&&e(this.link)||this.links.some(e)}static \u0275fac=function(i){return new(i||t)(ne(Un),ne(O),ne(Be),ne(Pe))};static \u0275dir=C({type:t,selectors:[["","routerLinkActive",""]],contentQueries:function(i,r,o){if(i&1&&mt(o,ta,5),i&2){let s;q(s=Y())&&(r.links=s)}},inputs:{routerLinkActiveOptions:"routerLinkActiveOptions",ariaCurrentWhenActive:"ariaCurrentWhenActive",routerLinkActive:"routerLinkActive"},outputs:{isActiveChange:"isActiveChange"},exportAs:["routerLinkActive"],features:[Ie]})}return t})();function aB(t){let n=t;return!!(n.paths||n.matrixParams||n.queryParams||n.fragment)}var lB=new g("");function Iy(t,...n){return ei([{provide:uc,multi:!0,useValue:t},[],{provide:Hi,useFactory:cB},{provide:yr,multi:!0,useFactory:uB},n.map(e=>e.\u0275providers)])}function cB(){return d(Un).routerState.root}function dB(t,n){return{\u0275kind:t,\u0275providers:n}}function uB(){let t=d(W);return n=>{let e=t.get(ht);if(n!==e.components[0])return;let i=t.get(Un),r=t.get(fB);t.get(hB)===1&&i.initialNavigation(),t.get(mB,null,{optional:!0})?.setUpPreloading(),t.get(lB,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var fB=new g("",{factory:()=>new x}),hB=new g("",{factory:()=>1});var mB=new g("");function My(){return dB(8,[Dy,{provide:cc,useExisting:Dy}])}var Ty;try{Ty=typeof Intl<"u"&&Intl.v8BreakIterator}catch{Ty=!1}var Ce=(()=>{class t{_platformId=d(Ai);isBrowser=this._platformId?Nf(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||Ty)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ay;function KI(){if(Ay==null){let t=typeof document<"u"?document.head:null;Ay=!!(t&&(t.createShadowRoot||t.attachShadow))}return Ay}function Ry(t){if(KI()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function fc(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function Tt(t){return t.composedPath?t.composedPath()[0]:t.target}function ky(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var mh=new WeakMap,pt=(()=>{class t{_appRef;_injector=d(W);_environmentInjector=d(Ne);load(e){let i=this._appRef=this._appRef||this._injector.get(ht),r=mh.get(i);r||(r={loaders:new Set,refs:[]},mh.set(i,r),i.onDestroy(()=>{mh.get(i)?.refs.forEach(o=>o.destroy()),mh.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(Af(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function dt(t){return t==null?"":typeof t=="string"?t:`${t}px`}function na(t){return Array.isArray(t)?t:[t]}function mi(t,n=0){return QI(t)?Number(t):arguments.length===2?n:0}function QI(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function cn(t){return t instanceof O?t.nativeElement:t}var gB=new g("cdk-dir-doc",{providedIn:"root",factory:()=>d(P)}),vB=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function XI(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?vB.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var bt=(()=>{class t{get value(){return this.valueSignal()}valueSignal=k("ltr");change=new j;constructor(){let e=d(gB,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(XI(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Hn=(function(t){return t[t.NORMAL=0]="NORMAL",t[t.NEGATED=1]="NEGATED",t[t.INVERTED=2]="INVERTED",t})(Hn||{}),ph,Oo;function gh(){if(Oo==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return Oo=!1,Oo;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)Oo=!0;else{let t=Element.prototype.scrollTo;t?Oo=!/\{\s*\[native code\]\s*\}/.test(t.toString()):Oo=!1}}return Oo}function ia(){if(typeof document!="object"||!document)return Hn.NORMAL;if(ph==null){let t=document.createElement("div"),n=t.style;t.dir="rtl",n.width="1px",n.overflow="auto",n.visibility="hidden",n.pointerEvents="none",n.position="absolute";let e=document.createElement("div"),i=e.style;i.width="2px",i.height="1px",t.appendChild(e),document.body.appendChild(t),ph=Hn.NORMAL,t.scrollLeft===0&&(t.scrollLeft=1,ph=t.scrollLeft===0?Hn.NEGATED:Hn.INVERTED),t.remove()}return ph}function vh(t){return t&&typeof t.connect=="function"&&!(t instanceof ja)}var zn=(function(t){return t[t.REPLACED=0]="REPLACED",t[t.INSERTED=1]="INSERTED",t[t.MOVED=2]="MOVED",t[t.REMOVED=3]="REMOVED",t})(zn||{}),_h=class{viewCacheSize=20;_viewCache=[];applyChanges(n,e,i,r,o){n.forEachOperation((s,a,l)=>{let c,u;if(s.previousIndex==null){let f=()=>i(s,a,l);c=this._insertView(f,l,e,r(s)),u=c?zn.INSERTED:zn.REPLACED}else l==null?(this._detachAndCacheView(a,e),u=zn.REMOVED):(c=this._moveView(a,l,e,r(s)),u=zn.MOVED);o&&o({context:c?.context,operation:u,record:s})})}detach(){for(let n of this._viewCache)n.destroy();this._viewCache=[]}_insertView(n,e,i,r){let o=this._insertViewFromCache(e,i);if(o){o.context.$implicit=r;return}let s=n();return i.createEmbeddedView(s.templateRef,s.context,s.index)}_detachAndCacheView(n,e){let i=e.detach(n);this._maybeCacheView(i,e)}_moveView(n,e,i,r){let o=i.get(n);return i.move(o,e),o.context.$implicit=r,o}_maybeCacheView(n,e){if(this._viewCache.length<this.viewCacheSize)this._viewCache.push(n);else{let i=e.indexOf(n);i===-1?n.destroy():e.remove(i)}}_insertViewFromCache(n,e){let i=this._viewCache.pop();return i&&e.insert(i,n),i||null}};var ye=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({})}return t})();var _B=20,hc=(()=>{class t{_ngZone=d(G);_platform=d(Ce);_renderer=d(ft).createRenderer(null,null);_cleanupGlobalListener;constructor(){}_scrolled=new x;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=_B){return this._platform.isBrowser?new de(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(as(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):B()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(be(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._scrollableContainsElement(o,e)&&i.push(o)}),i}_scrollableContainsElement(e,i){let r=cn(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),JI=(()=>{class t{elementRef=d(O);scrollDispatcher=d(hc);ngZone=d(G);dir=d(bt,{optional:!0});_scrollElement=this.elementRef.nativeElement;_destroyed=new x;_renderer=d(Be);_cleanupScroll;_elementScrolled=new x;constructor(){}ngOnInit(){this._cleanupScroll=this.ngZone.runOutsideAngular(()=>this._renderer.listen(this._scrollElement,"scroll",e=>this._elementScrolled.next(e))),this.scrollDispatcher.register(this)}ngOnDestroy(){this._cleanupScroll?.(),this._elementScrolled.complete(),this.scrollDispatcher.deregister(this),this._destroyed.next(),this._destroyed.complete()}elementScrolled(){return this._elementScrolled}getElementRef(){return this.elementRef}scrollTo(e){let i=this.elementRef.nativeElement,r=this.dir&&this.dir.value=="rtl";e.left==null&&(e.left=r?e.end:e.start),e.right==null&&(e.right=r?e.start:e.end),e.bottom!=null&&(e.top=i.scrollHeight-i.clientHeight-e.bottom),r&&ia()!=Hn.NORMAL?(e.left!=null&&(e.right=i.scrollWidth-i.clientWidth-e.left),ia()==Hn.INVERTED?e.left=e.right:ia()==Hn.NEGATED&&(e.left=e.right?-e.right:e.right)):e.right!=null&&(e.left=i.scrollWidth-i.clientWidth-e.right),this._applyScrollToOptions(e)}_applyScrollToOptions(e){let i=this.elementRef.nativeElement;gh()?i.scrollTo(e):(e.top!=null&&(i.scrollTop=e.top),e.left!=null&&(i.scrollLeft=e.left))}measureScrollOffset(e){let i="left",r="right",o=this.elementRef.nativeElement;if(e=="top")return o.scrollTop;if(e=="bottom")return o.scrollHeight-o.clientHeight-o.scrollTop;let s=this.dir&&this.dir.value=="rtl";return e=="start"?e=s?r:i:e=="end"&&(e=s?i:r),s&&ia()==Hn.INVERTED?e==i?o.scrollWidth-o.clientWidth-o.scrollLeft:o.scrollLeft:s&&ia()==Hn.NEGATED?e==i?o.scrollLeft+o.scrollWidth-o.clientWidth:-o.scrollLeft:e==i?o.scrollLeft:o.scrollWidth-o.clientWidth-o.scrollLeft}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","cdk-scrollable",""],["","cdkScrollable",""]]})}return t})(),yB=20,pi=(()=>{class t{_platform=d(Ce);_listeners;_viewportSize=null;_change=new x;_document=d(P);constructor(){let e=d(G),i=d(ft).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),s=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(e=yB){return e>0?this._change.pipe(as(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var eM=new g("CDK_VIRTUAL_SCROLL_VIEWPORT");var ra=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({})}return t})(),mc=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[ye,ra,ye,ra]})}return t})();var Oy={},Fe=class t{_appId=d(_n);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){return this._appId!=="ng"&&(n+=this._appId),Oy.hasOwnProperty(n)||(Oy[n]=0),`${n}${e?t._infix+"-":""}${Oy[n]++}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})};var pc=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},zi=class extends pc{component;viewContainerRef;injector;projectableNodes;bindings;constructor(n,e,i,r,o){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null}},dn=class extends pc{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},Ny=class extends pc{element;constructor(n){super(),this.element=n instanceof O?n.nativeElement:n}},Tr=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof zi)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof dn)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof Ny)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},gc=class extends Tr{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(ci,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||W.NULL,o=r.get(Ne,i.injector);e=Af(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var gi=(()=>{class t extends Tr{_moduleRef=d(ci,{optional:!0});_document=d(P);_viewContainerRef=d(Qe);_isInitialized=!1;_attachedRef=null;constructor(){super()}get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new j;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[oe]})}return t})(),vi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({})}return t})();function xt(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var tM=gh();function aa(t){return new yh(t.get(pi),t.get(P))}var yh=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=dt(-this._previousScrollPosition.left),n.style.top=dt(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",s=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),tM&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),tM&&(i.scrollBehavior=o,r.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function lM(t,n){return new bh(t.get(hc),t.get(G),t.get(pi),n)}var bh=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(be(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var vc=class{enable(){}disable(){}attach(){}};function Py(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,s=t.left>e.right;return i||r||o||s})}function nM(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,s=t.right>e.right;return i||r||o||s})}function Fo(t,n){return new Dh(t.get(hc),t.get(pi),t.get(G),n)}var Dh=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();Py(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},cM=(()=>{class t{_injector=d(W);constructor(){}noop=()=>new vc;close=e=>lM(this._injector,e);block=()=>aa(this._injector);reposition=e=>Fo(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),$n=class{positionStrategy;scrollStrategy=new vc;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var wh=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var dM=(()=>{class t{_attachedOverlays=[];_document=d(P);_isAttached=!1;constructor(){}ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),uM=(()=>{class t extends dM{_ngZone=d(G);_renderer=d(ft).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),fM=(()=>{class t extends dM{_platform=d(Ce);_ngZone=d(G);_renderer=d(ft).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=Tt(e)};_clickListener=e=>{let i=Tt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],l=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,e,l))){if(iM(a.overlayElement,i)||iM(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>l.next(e)):l.next(e)}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function iM(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var hM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),xh=(()=>{class t{_platform=d(Ce);_containerElement;_document=d(P);_styleLoader=d(pt);constructor(){}ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||ky()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),ky()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(hM)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Fy=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function Ly(t){return t&&t.nodeType===1}var oa=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new x;_attachments=new x;_detachments=new x;_positionStrategy;_scrollStrategy;_locationChanges=ae.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new x;_outsidePointerEvents=new x;_afterNextRenderRef;constructor(n,e,i,r,o,s,a,l,c,u=!1,f,h){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=l,this._outsideClickDispatcher=c,this._animationsDisabled=u,this._injector=f,this._renderer=h,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=Ke(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=y(y({},this._config),n),this._updateElementSize()}setDirection(n){this._config=te(y({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=dt(this._config.width),n.height=dt(this._config.height),n.minWidth=dt(this._config.minWidth),n.minHeight=dt(this._config.minHeight),n.maxWidth=dt(this._config.maxWidth),n.maxHeight=dt(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;Ly(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new Fy(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=na(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=Ke(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},rM="cdk-overlay-connected-position-bounding-box",DB=/([A-Za-z%]+)$/;function _c(t,n){return new Ch(n,t.get(pi),t.get(P),t.get(Ce),t.get(xh))}var Ch=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new x;_resizeSubscription=ae.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(rM),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let l=this._getOriginPoint(n,r,a),c=this._getOverlayPoint(l,e,a),u=this._getOverlayFit(c,e,i,a);if(u.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,l);return}if(this._canFitWithFlexibleDimensions(u,c,i)){o.push({position:a,origin:l,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(l,a)});continue}(!s||s.overlayFit.visibleArea<u.visibleArea)&&(s={overlayFit:u,overlayPoint:c,originPoint:l,position:a,overlayRect:e})}if(o.length){let a=null,l=-1;for(let c of o){let u=c.boundingBoxRect.width*c.boundingBoxRect.height*(c.position.weight||1);u>l&&(l=u,a=c)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&No(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(rM),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof O?this._origin.nativeElement:Ly(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let s=this._isRtl()?n.right:n.left,a=this._isRtl()?n.left:n.right;r=i.originX=="start"?s:a}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=sM(e),{x:s,y:a}=n,l=this._getOffset(r,"x"),c=this._getOffset(r,"y");l&&(s+=l),c&&(a+=c);let u=0-s,f=s+o.width-i.width,h=0-a,m=a+o.height-i.height,p=this._subtractOverflows(o.width,u,f),w=this._subtractOverflows(o.height,h,m),E=p*w;return{visibleArea:E,isCompletelyWithinViewport:o.width*o.height===E,fitsInViewportVertically:w===o.height,fitsInViewportHorizontally:p==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,s=oM(this._overlayRef.getConfig().minHeight),a=oM(this._overlayRef.getConfig().minWidth),l=n.fitsInViewportVertically||s!=null&&s<=r,c=n.fitsInViewportHorizontally||a!=null&&a<=o;return l&&c}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=sM(e),o=this._viewportRect,s=Math.max(n.x+r.width-o.width,0),a=Math.max(n.y+r.height-o.height,0),l=Math.max(o.top-i.top-n.y,0),c=Math.max(o.left-i.left-n.x,0),u=0,f=0;return r.width<=o.width?u=c||-s:u=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?f=l||-a:f=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:u,y:f},{x:n.x+u,y:n.y+f}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!wB(this._lastScrollVisibility,i)){let r=new wh(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(e.overlayY==="top")s=n.y,o=i.height-s+this._getViewportMarginBottom();else if(e.overlayY==="bottom")a=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let m=Math.min(i.bottom-n.y+i.top,n.y),p=this._lastBoundingBoxSize.height;o=m*2,s=n.y-m,o>p&&!this._isInitialRender&&!this._growAfterOpen&&(s=n.y-p/2)}let l=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,c=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,u,f,h;if(c)h=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),u=n.x-this._getViewportMarginStart();else if(l)f=n.x,u=i.right-n.x-this._getViewportMarginEnd();else{let m=Math.min(i.right-n.x+i.left,n.x),p=this._lastBoundingBoxSize.width;u=m*2,f=n.x-m,u>p&&!this._isInitialRender&&!this._growAfterOpen&&(f=n.x-p/2)}return{top:s,left:f,bottom:a,right:h,width:u,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=dt(i.width),r.height=dt(i.height),r.top=dt(i.top)||"auto",r.bottom=dt(i.bottom)||"auto",r.left=dt(i.left)||"auto",r.right=dt(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=dt(o)),s&&(r.maxWidth=dt(s))}this._lastBoundingBoxSize=i,No(this._boundingBox.style,r)}_resetBoundingBoxStyles(){No(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){No(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let u=this._viewportRuler.getViewportScrollPosition();No(i,this._getExactOverlayY(e,n,u)),No(i,this._getExactOverlayX(e,n,u))}else i.position="static";let a="",l=this._getOffset(e,"x"),c=this._getOffset(e,"y");l&&(a+=`translateX(${l}px) `),c&&(a+=`translateY(${c}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=dt(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=dt(s.maxWidth):o&&(i.maxWidth="")),No(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=dt(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=n.overlayX==="end"?"left":"right":s=n.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=dt(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:nM(n,i),isOriginOutsideView:Py(n,i),isOverlayClipped:nM(e,i),isOverlayOutsideView:Py(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&na(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof O)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function No(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function oM(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(DB);return!e||e==="px"?parseFloat(n):null}return t||null}function sM(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function wB(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var aM="cdk-global-overlay-wrapper";function Ar(t){return new Eh}var Eh=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(aM),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,l=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),c=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),u=this._xPosition,f=this._xOffset,h=this._overlayRef.getConfig().direction==="rtl",m="",p="",w="";l?w="flex-start":u==="center"?(w="center",h?p=f:m=f):h?u==="left"||u==="end"?(w="flex-end",m=f):(u==="right"||u==="start")&&(w="flex-start",p=f):u==="left"||u==="start"?(w="flex-start",m=f):(u==="right"||u==="end")&&(w="flex-end",p=f),n.position=this._cssPosition,n.marginLeft=l?"0":m,n.marginTop=c?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=l?"0":p,e.justifyContent=w,e.alignItems=c?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(aM),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},mM=(()=>{class t{_injector=d(W);constructor(){}global(){return Ar()}flexibleConnectedTo(e){return _c(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),yc=new g("OVERLAY_DEFAULT_CONFIG");function $i(t,n){t.get(pt).load(hM);let e=t.get(xh),i=t.get(P),r=t.get(Fe),o=t.get(ht),s=t.get(bt),a=t.get(Be,null,{optional:!0})||t.get(ft).createRenderer(null,null),l=new $n(n),c=t.get(yc,null,{optional:!0})?.usePopover??!0;l.direction=l.direction||s.value,"showPopover"in i.body?l.usePopover=n?.usePopover??c:l.usePopover=!1;let u=i.createElement("div"),f=i.createElement("div");u.id=r.getId("cdk-overlay-"),u.classList.add("cdk-overlay-pane"),f.appendChild(u),l.usePopover&&(f.setAttribute("popover","manual"),f.classList.add("cdk-overlay-popover"));let h=l.usePopover?l.positionStrategy?.getPopoverInsertionPoint?.():null;return Ly(h)?h.after(f):h?.type==="parent"?h.element.appendChild(f):e.getContainerElement().appendChild(f),new oa(new gc(u,o,t),f,u,l,t.get(G),t.get(uM),i,t.get(xr),t.get(fM),n?.disableAnimations??t.get(xl,null,{optional:!0})==="NoopAnimations",t.get(Ne),a)}var pM=(()=>{class t{scrollStrategies=d(cM);_positionBuilder=d(mM);_injector=d(W);constructor(){}create(e){return $i(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),CB=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],EB=new g("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(W);return()=>Fo(t)}}),sa=(()=>{class t{elementRef=d(O);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),gM=new g("cdk-connected-overlay-default-config"),Sh=(()=>{class t{_dir=d(bt,{optional:!0});_injector=d(W);_overlayRef;_templatePortal;_backdropSubscription=ae.EMPTY;_attachSubscription=ae.EMPTY;_detachSubscription=ae.EMPTY;_positionSubscription=ae.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=d(EB);_ngZone=d(G);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new j;positionChange=new j;attach=new j;detach=new j;overlayKeydown=new j;overlayOutsideClick=new j;constructor(){let e=d(je),i=d(Qe),r=d(gM,{optional:!0}),o=d(yc,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new dn(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=CB);let e=this._overlayRef=$i(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!xt(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=Tt(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new $n({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=_c(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof sa?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof sa?this.origin.elementRef.nativeElement:this.origin instanceof O?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(np(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",L],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",L],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",L],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",L],push:[2,"cdkConnectedOverlayPush","push",L],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",L],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",L],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[Ie]})}return t})(),_i=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({providers:[pM],imports:[ye,vi,mc,mc]})}return t})();function Lo(t){return t.buttons===0||t.detail===0}function Vo(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var bc;function vM(){if(bc==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>bc=!0}))}finally{bc=bc||!1}return bc}function la(t){return vM()?t:!!t.capture}var _M=new g("cdk-input-modality-detector-options"),yM={ignoreKeys:[18,17,224,91,16]},bM=650,Vy={passive:!0,capture:!0},DM=(()=>{class t{_platform=d(Ce);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new rt(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=Tt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<bM||(this._modality.next(Lo(e)?"keyboard":"mouse"),this._mostRecentTarget=Tt(e))};_onTouchstart=e=>{if(Vo(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=Tt(e)};constructor(){let e=d(G),i=d(P),r=d(_M,{optional:!0});if(this._options=y(y({},yM),r),this.modalityDetected=this._modality.pipe(Ya(1)),this.modalityChanged=this.modalityDetected.pipe(Nd()),this._platform.isBrowser){let o=d(ft).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,Vy),o.listen(i,"mousedown",this._onMousedown,Vy),o.listen(i,"touchstart",this._onTouchstart,Vy)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Dc=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(Dc||{}),wM=new g("cdk-focus-monitor-default-options"),Ih=la({passive:!0,capture:!0}),wn=(()=>{class t{_ngZone=d(G);_platform=d(Ce);_inputModalityDetector=d(DM);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=d(P);_stopInputModalityDetector=new x;constructor(){let e=d(wM,{optional:!0});this._detectionMode=e?.detectionMode||Dc.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=Tt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=cn(e);if(!this._platform.isBrowser||r.nodeType!==1)return B();let o=Ry(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new x,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let i=cn(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=cn(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,l])=>this._originChanged(a,i,l)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===Dc.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===Dc.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?bM:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=Tt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,Ih),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,Ih)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(Ae(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Ih),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Ih),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Th=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2,changeDetection:0})}return t})(),Mh;function xB(){if(Mh===void 0&&(Mh=null,typeof window<"u")){let t=window;t.trustedTypes!==void 0&&(Mh=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return Mh}function jo(t){return xB()?.createHTML(t)||t}function CM(t,n,e){let i=e.sanitize(_t.HTML,n);t.innerHTML=jo(i||"")}var EM=new Set,Bo,Ah=(()=>{class t{_platform=d(Ce);_nonce=d(wo,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):IB}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&SB(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function SB(t,n){if(!EM.has(t))try{Bo||(Bo=document.createElement("style"),n&&Bo.setAttribute("nonce",n),Bo.setAttribute("type","text/css"),document.head.appendChild(Bo)),Bo.sheet&&(Bo.sheet.insertRule(`@media ${t} {body{ }}`,0),EM.add(t))}catch(e){console.error(e)}}function IB(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var wc=(()=>{class t{_mediaMatcher=d(Ah);_zone=d(G);_queries=new Map;_destroySubject=new x;constructor(){}ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return xM(na(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=xM(na(e)).map(s=>this._registerQuery(s).observable),o=Kn(r);return o=sr(o.pipe(ze(1)),o.pipe(Ya(1),Wa(0))),o.pipe(fe(s=>{let a={matches:!1,breakpoints:{}};return s.forEach(({matches:l,query:c})=>{a.matches=a.matches||l,a.breakpoints[c]=l}),a}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new de(s=>{let a=l=>this._zone.run(()=>s.next(l));return i.addListener(a),()=>{i.removeListener(a)}}).pipe(Je(i),fe(({matches:s})=>({query:e,matches:s})),Ae(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function xM(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}var MB=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Rh=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({providers:[MB]})}return t})();var Uy=(()=>{class t{_platform=d(Ce);constructor(){}isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return AB(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=TB(VB(e));if(i&&(SM(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=SM(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!FB(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return LB(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function TB(t){try{return t.frameElement}catch{return null}}function AB(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function RB(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function kB(t){return NB(t)&&t.type=="hidden"}function OB(t){return PB(t)&&t.hasAttribute("href")}function NB(t){return t.nodeName.toLowerCase()=="input"}function PB(t){return t.nodeName.toLowerCase()=="a"}function TM(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function SM(t){if(!TM(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function FB(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function LB(t){return kB(t)?!1:RB(t)||OB(t)||t.hasAttribute("contenteditable")||TM(t)}function VB(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var By=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>this.focusLastTabbableElement();endAnchorListener=()=>this.focusFirstTabbableElement();get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,s){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=s,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){this._injector?Ke(n,{injector:this._injector}):setTimeout(n)}},Hy=(()=>{class t{_checker=d(Uy);_ngZone=d(G);_document=d(P);_injector=d(W);constructor(){d(pt).load(Th)}create(e,i=!1){return new By(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var AM=new g("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),RM=new g("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),jB=0,Cc=(()=>{class t{_ngZone=d(G);_defaultOptions=d(RM,{optional:!0});_liveElement;_document=d(P);_sanitizer=d(ql);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=d(AM,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,s;return i.length===1&&typeof i[0]=="number"?s=i[0]:[o,s]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),s==null&&r&&(s=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:CM(this._liveElement,e,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${jB++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Rr=(function(t){return t[t.NONE=0]="NONE",t[t.BLACK_ON_WHITE=1]="BLACK_ON_WHITE",t[t.WHITE_ON_BLACK=2]="WHITE_ON_BLACK",t})(Rr||{}),IM="cdk-high-contrast-black-on-white",MM="cdk-high-contrast-white-on-black",jy="cdk-high-contrast-active",kM=(()=>{class t{_platform=d(Ce);_hasCheckedHighContrastMode=!1;_document=d(P);_breakpointSubscription;constructor(){this._breakpointSubscription=d(wc).observe("(forced-colors: active)").subscribe(()=>{this._hasCheckedHighContrastMode&&(this._hasCheckedHighContrastMode=!1,this._applyBodyHighContrastModeCssClasses())})}getHighContrastMode(){if(!this._platform.isBrowser)return Rr.NONE;let e=this._document.createElement("div");e.style.backgroundColor="rgb(1,2,3)",e.style.position="absolute",this._document.body.appendChild(e);let i=this._document.defaultView||window,r=i&&i.getComputedStyle?i.getComputedStyle(e):null,o=(r&&r.backgroundColor||"").replace(/ /g,"");switch(e.remove(),o){case"rgb(0,0,0)":case"rgb(45,50,54)":case"rgb(32,32,32)":return Rr.WHITE_ON_BLACK;case"rgb(255,255,255)":case"rgb(255,250,239)":return Rr.BLACK_ON_WHITE}return Rr.NONE}ngOnDestroy(){this._breakpointSubscription.unsubscribe()}_applyBodyHighContrastModeCssClasses(){if(!this._hasCheckedHighContrastMode&&this._platform.isBrowser&&this._document.body){let e=this._document.body.classList;e.remove(jy,IM,MM),this._hasCheckedHighContrastMode=!0;let i=this.getHighContrastMode();i===Rr.BLACK_ON_WHITE?e.add(jy,IM):i===Rr.WHITE_ON_BLACK&&e.add(jy,MM)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),zy=(()=>{class t{constructor(){d(kM)._applyBodyHighContrastModeCssClasses()}static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[Rh]})}return t})();function BB(t,n){}var kr=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext};var Gy=(()=>{class t extends Tr{_elementRef=d(O);_focusTrapFactory=d(Hy);_config;_interactivityChecker=d(Uy);_ngZone=d(G);_focusMonitor=d(wn);_renderer=d(Be);_changeDetectorRef=d(Pe);_injector=d(W);_platform=d(Ce);_document=d(P);_portalOutlet;_focusTrapped=new x;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=d(kr,{optional:!0})||new kr,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),s(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),s=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||Ke(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=fc(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=fc();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=fc()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(i,r){if(i&1&&ct(gi,7),i&2){let o;q(o=Y())&&(r._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(i,r){i&2&&le("id",r._config.id||null)("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null)},features:[oe],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&Me(0,BB,0,0,"ng-template",0)},dependencies:[gi],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2})}return t})(),Ec=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new x;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!xt(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},UB=new g("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=d(W);return()=>aa(t)}}),HB=new g("DialogData"),zB=new g("DefaultDialogConfig");function $B(t){let n=k(t),e=new j;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var Wy=(()=>{class t{_injector=d(W);_defaultOptions=d(zB,{optional:!0});_parentDialog=d(t,{optional:!0,skipSelf:!0});_overlayContainer=d(xh);_idGenerator=d(Fe);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new x;_afterOpenedAtThisLevel=new x;_ariaHiddenElements=new Map;_scrollStrategy=d(UB);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=xn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(Je(void 0)));constructor(){}open(e,i){let r=this._defaultOptions||new kr;i=y(y({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),s=$i(this._injector,o),a=new Ec(s,i),l=this._attachContainer(s,a,i);if(a.containerInstance=l,!this.openDialogs.length){let c=this._overlayContainer.getContainerElement();l._focusTrapped?l._focusTrapped.pipe(ze(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(c)}):this._hideNonDialogContentFromAssistiveTechnology(c)}return this._attachDialogContent(e,a,l,i),this.openDialogs.push(a),a.closed.subscribe(()=>this._removeOpenDialog(a,!0)),this.afterOpened.next(a),a}closeAll(){$y(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){$y(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),$y(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new $n({positionStrategy:e.positionStrategy||Ar().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,s=[{provide:kr,useValue:r},{provide:Ec,useValue:i},{provide:oa,useValue:e}],a;r.container?typeof r.container=="function"?a=r.container:(a=r.container.type,s.push(...r.container.providers(r))):a=Gy;let l=new zi(a,r.viewContainerRef,W.create({parent:o||this._injector,providers:s}));return e.attach(l).instance}_attachDialogContent(e,i,r,o){if(e instanceof je){let s=this._createInjector(o,i,r,void 0),a={$implicit:o.data,dialogRef:i};o.templateContext&&(a=y(y({},a),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new dn(e,null,a,s))}else{let s=this._createInjector(o,i,r,this._injector),a=r.attachComponentPortal(new zi(e,o.viewContainerRef,s));i.componentRef=a,i.componentInstance=a.instance}}_createInjector(e,i,r,o){let s=e.injector||e.viewContainerRef?.injector,a=[{provide:HB,useValue:e.data},{provide:Ec,useValue:i}];return e.providers&&(typeof e.providers=="function"?a.push(...e.providers(i,e,r)):a.push(...e.providers)),e.direction&&(!s||!s.get(bt,null,{optional:!0}))&&a.push({provide:bt,useValue:$B(e.direction)}),W.create({parent:s||o,providers:a})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,s)=>{o?s.setAttribute("aria-hidden",o):s.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function $y(t,n){let e=t.length;for(;e--;)n(t[e])}var OM=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({providers:[Wy],imports:[_i,vi,zy,vi]})}return t})();function Gi(t){return t!=null&&`${t}`!="false"}var NM={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var GB=new g("MATERIAL_ANIMATIONS"),PM=null;function WB(){return d(GB,{optional:!0})?.animationsDisabled||d(xl,{optional:!0})==="NoopAnimations"?"di-disabled":(PM??=d(Ah).matchMedia("(prefers-reduced-motion)").matches,PM?"reduced-motion":"enabled")}function Ge(){return WB()!=="enabled"}var qB=200,kh=class{_letterKeyStream=new x;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new x;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:qB;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(Dt(e=>this._pressedLetters.push(e)),Wa(n),be(()=>this._pressedLetters.length>0),fe(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};var ca=class{_items;_activeItemIndex=k(-1);_activeItem=k(null);_wrap=!1;_typeaheadSubscription=ae.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof Pn?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):di(n)&&(this._effectRef=kn(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new x;change=new x;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new kh(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||xt(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return di(this._items)?this._items():this._items instanceof Pn?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var Tc=class extends ca{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var Uo=class extends ca{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var LM=" ";function VM(t,n,e){let i=jM(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(LM)))}function qy(t,n,e){let i=jM(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(LM)):t.removeAttribute(n)}function jM(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}function YB(t,n){}var da=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration},Yy="mdc-dialog--open",BM="mdc-dialog--opening",UM="mdc-dialog--closing",ZB=150,KB=75,QB=(()=>{class t extends Gy{_animationStateChanged=new j;_animationsEnabled=!Ge();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?zM(this._config.enterAnimationDuration)??ZB:0;_exitAnimationDuration=this._animationsEnabled?zM(this._config.exitAnimationDuration)??KB:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(HM,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(BM,Yy)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(Yy),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(Yy),this._animationsEnabled?(this._hostElement.style.setProperty(HM,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(UM)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(BM,UM)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(i,r){i&2&&(Gt("id",r._config.id),le("aria-modal",r._config.ariaModal)("role",r._config.role)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null),Z("_mat-animation-noopable",!r._animationsEnabled)("mat-mdc-dialog-container-with-actions",r._actionSectionCount>0))},features:[oe],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(v(0,"div",0)(1,"div",1),Me(2,YB,0,0,"ng-template",2),_()())},dependencies:[gi],styles:[`.mat-mdc-dialog-container {
  width: 100%;
  height: 100%;
  display: block;
  box-sizing: border-box;
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  outline: 0;
}

.cdk-overlay-pane.mat-mdc-dialog-panel {
  max-width: var(--mat-dialog-container-max-width, 560px);
  min-width: var(--mat-dialog-container-min-width, 280px);
}
@media (max-width: 599px) {
  .cdk-overlay-pane.mat-mdc-dialog-panel {
    max-width: var(--mat-dialog-container-small-max-width, calc(100vw - 32px));
  }
}

.mat-mdc-dialog-inner-container {
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-around;
  box-sizing: border-box;
  height: 100%;
  opacity: 0;
  transition: opacity linear var(--mat-dialog-transition-duration, 0ms);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
}
.mdc-dialog--closing .mat-mdc-dialog-inner-container {
  transition: opacity 75ms linear;
  transform: none;
}
.mdc-dialog--open .mat-mdc-dialog-inner-container {
  opacity: 1;
}
._mat-animation-noopable .mat-mdc-dialog-inner-container {
  transition: none;
}

.mat-mdc-dialog-surface {
  display: flex;
  flex-direction: column;
  flex-grow: 0;
  flex-shrink: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  position: relative;
  overflow-y: auto;
  outline: 0;
  transform: scale(0.8);
  transition: transform var(--mat-dialog-transition-duration, 0ms) cubic-bezier(0, 0, 0.2, 1);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  box-shadow: var(--mat-dialog-container-elevation-shadow, none);
  border-radius: var(--mat-dialog-container-shape, var(--mat-sys-corner-extra-large, 4px));
  background-color: var(--mat-dialog-container-color, var(--mat-sys-surface, white));
}
[dir=rtl] .mat-mdc-dialog-surface {
  text-align: right;
}
.mdc-dialog--open .mat-mdc-dialog-surface, .mdc-dialog--closing .mat-mdc-dialog-surface {
  transform: none;
}
._mat-animation-noopable .mat-mdc-dialog-surface {
  transition: none;
}
.mat-mdc-dialog-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 2px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}

.mat-mdc-dialog-title {
  display: block;
  position: relative;
  flex-shrink: 0;
  box-sizing: border-box;
  margin: 0 0 1px;
  padding: var(--mat-dialog-headline-padding, 6px 24px 13px);
}
.mat-mdc-dialog-title::before {
  display: inline-block;
  width: 0;
  height: 40px;
  content: "";
  vertical-align: 0;
}
[dir=rtl] .mat-mdc-dialog-title {
  text-align: right;
}
.mat-mdc-dialog-container .mat-mdc-dialog-title {
  color: var(--mat-dialog-subhead-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-dialog-subhead-font, var(--mat-sys-headline-small-font, inherit));
  line-height: var(--mat-dialog-subhead-line-height, var(--mat-sys-headline-small-line-height, 1.5rem));
  font-size: var(--mat-dialog-subhead-size, var(--mat-sys-headline-small-size, 1rem));
  font-weight: var(--mat-dialog-subhead-weight, var(--mat-sys-headline-small-weight, 400));
  letter-spacing: var(--mat-dialog-subhead-tracking, var(--mat-sys-headline-small-tracking, 0.03125em));
}

.mat-mdc-dialog-content {
  display: block;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  overflow: auto;
  max-height: 65vh;
}
.mat-mdc-dialog-content > :first-child {
  margin-top: 0;
}
.mat-mdc-dialog-content > :last-child {
  margin-bottom: 0;
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  color: var(--mat-dialog-supporting-text-color, var(--mat-sys-on-surface-variant, rgba(0, 0, 0, 0.6)));
  font-family: var(--mat-dialog-supporting-text-font, var(--mat-sys-body-medium-font, inherit));
  line-height: var(--mat-dialog-supporting-text-line-height, var(--mat-sys-body-medium-line-height, 1.5rem));
  font-size: var(--mat-dialog-supporting-text-size, var(--mat-sys-body-medium-size, 1rem));
  font-weight: var(--mat-dialog-supporting-text-weight, var(--mat-sys-body-medium-weight, 400));
  letter-spacing: var(--mat-dialog-supporting-text-tracking, var(--mat-sys-body-medium-tracking, 0.03125em));
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  padding: var(--mat-dialog-content-padding, 20px 24px);
}
.mat-mdc-dialog-container-with-actions .mat-mdc-dialog-content {
  padding: var(--mat-dialog-with-actions-content-padding, 20px 24px 0);
}
.mat-mdc-dialog-container .mat-mdc-dialog-title + .mat-mdc-dialog-content {
  padding-top: 0;
}

.mat-mdc-dialog-actions {
  display: flex;
  position: relative;
  flex-shrink: 0;
  flex-wrap: wrap;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  margin: 0;
  border-top: 1px solid transparent;
  padding: var(--mat-dialog-actions-padding, 16px 24px);
  justify-content: var(--mat-dialog-actions-alignment, flex-end);
}
@media (forced-colors: active) {
  .mat-mdc-dialog-actions {
    border-top-color: CanvasText;
  }
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-start, .mat-mdc-dialog-actions[align=start] {
  justify-content: start;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-center, .mat-mdc-dialog-actions[align=center] {
  justify-content: center;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-end, .mat-mdc-dialog-actions[align=end] {
  justify-content: flex-end;
}
.mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
.mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 8px;
}
[dir=rtl] .mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
[dir=rtl] .mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 0;
  margin-right: 8px;
}

.mat-mdc-dialog-component-host {
  display: contents;
}
`],encapsulation:2})}return t})(),HM="--mat-dialog-transition-duration";function zM(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?mi(t.substring(0,t.length-2)):t.endsWith("s")?mi(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var Oh=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})(Oh||{}),Ho=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new nr(1);_beforeClosed=new nr(1);_result;_closeFallbackTimeout;_state=Oh.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(be(r=>r.state==="opened"),ze(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(be(r=>r.state==="closed"),ze(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),Lt(this.backdropClick(),this.keydownEvents().pipe(be(r=>r.keyCode===27&&!this.disableClose&&!xt(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),XB(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(be(i=>i.state==="closing"),ze(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=Oh.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=Oh.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function XB(t,n,e){return t._closeInteractionType=n,t.close(e)}var Ac=new g("MatMdcDialogData"),Zy=new g("mat-mdc-dialog-default-options"),JB=new g("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(W);return()=>aa(t)}}),Rc=(()=>{class t{_defaultOptions=d(Zy,{optional:!0});_scrollStrategy=d(JB);_parentDialog=d(t,{optional:!0,skipSelf:!0});_idGenerator=d(Fe);_injector=d(W);_dialog=d(Wy);_animationsDisabled=Ge();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new x;_afterOpenedAtThisLevel=new x;dialogConfigClass=da;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=xn(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(Je(void 0)));constructor(){this._dialogRefConstructor=Ho,this._dialogContainerType=QB,this._dialogDataToken=Ac}open(e,i){let r;i=y(y({},this._defaultOptions||new da),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,te(y({},i),{positionStrategy:Ar(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:kr,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(s,a,l)=>(r=new this._dialogRefConstructor(s,i,l),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:l},{provide:this._dialogDataToken,useValue:a.data},{provide:this._dialogRefConstructor,useValue:r}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let s=this.openDialogs.indexOf(r);s>-1&&(this.openDialogs.splice(s,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Nh=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","mat-dialog-content",""],["mat-dialog-content"],["","matDialogContent",""]],hostAttrs:[1,"mat-mdc-dialog-content","mdc-dialog__content"],features:[t_([JI])]})}return t})();var $M=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({providers:[Rc],imports:[OM,_i,vi,ye]})}return t})();var ua,WM=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Ky(){if(ua)return ua;if(typeof document!="object"||!document)return ua=new Set(WM),ua;let t=document.createElement("input");return ua=new Set(WM.filter(n=>(t.setAttribute("type",n),t.type===n))),ua}var Cn=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(Cn||{}),Qy=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=Cn.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},qM=la({passive:!0,capture:!0}),Xy=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,qM)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,qM)))}_delegateEventHandler=n=>{let e=Tt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},kc={enterDuration:225,exitDuration:150},e2=800,YM=la({passive:!0,capture:!0}),ZM=["mousedown","touchstart"],KM=["mouseup","mouseleave","touchend","touchcancel"],t2=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2,changeDetection:0})}return t})(),Oc=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Xy;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=cn(i)),o&&o.get(pt).load(t2)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=y(y({},kc),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let s=i.radius||n2(n,e,r),a=n-r.left,l=e-r.top,c=o.enterDuration,u=document.createElement("div");u.classList.add("mat-ripple-element"),u.style.left=`${a-s}px`,u.style.top=`${l-s}px`,u.style.height=`${s*2}px`,u.style.width=`${s*2}px`,i.color!=null&&(u.style.backgroundColor=i.color),u.style.transitionDuration=`${c}ms`,this._containerElement.appendChild(u);let f=window.getComputedStyle(u),h=f.transitionProperty,m=f.transitionDuration,p=h==="none"||m==="0s"||m==="0s, 0s"||r.width===0&&r.height===0,w=new Qy(this,u,i,p);u.style.transform="scale3d(1, 1, 1)",w.state=Cn.FADING_IN,i.persistent||(this._mostRecentTransientRipple=w);let E=null;return!p&&(c||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let I=()=>{E&&(E.fallbackTimer=null),clearTimeout(nt),this._finishRippleTransition(w)},ce=()=>this._destroyRipple(w),nt=setTimeout(ce,c+100);u.addEventListener("transitionend",I),u.addEventListener("transitioncancel",ce),E={onTransitionEnd:I,onTransitionCancel:ce,fallbackTimer:nt}}),this._activeRipples.set(w,E),(p||!c)&&this._finishRippleTransition(w),w}fadeOutRipple(n){if(n.state===Cn.FADING_OUT||n.state===Cn.HIDDEN)return;let e=n.element,i=y(y({},kc),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=Cn.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=cn(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,ZM.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{KM.forEach(e=>{this._triggerElement.addEventListener(e,this,YM)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===Cn.FADING_IN?this._startFadeOutTransition(n):n.state===Cn.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=Cn.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=Cn.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=Lo(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+e2;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!Vo(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===Cn.VISIBLE||n.config.terminateOnPointerUp&&n.state===Cn.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(ZM.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(KM.forEach(e=>n.removeEventListener(e,this,YM)),this._pointerUpEventsRegistered=!1))}};function n2(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var Jy=new g("mat-ripple-global-options"),fa=(()=>{class t{_elementRef=d(O);_animationsDisabled=Ge();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=d(G),i=d(Ce),r=d(Jy,{optional:!0}),o=d(W);this._globalOptions=r||{},this._rippleRenderer=new Oc(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:y(y(y({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,y(y({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,y(y({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&Z("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var i2={capture:!0},r2=["focus","mousedown","mouseenter","touchstart"],eb="mat-ripple-loader-uninitialized",tb="mat-ripple-loader-class-name",QM="mat-ripple-loader-centered",Ph="mat-ripple-loader-disabled",XM=(()=>{class t{_document=d(P);_animationsDisabled=Ge();_globalRippleOptions=d(Jy,{optional:!0});_platform=d(Ce);_ngZone=d(G);_injector=d(W);_eventCleanups;_hosts=new Map;constructor(){let e=d(ft).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>r2.map(i=>e.listen(this._document,i,this._onInteraction,i2)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(eb,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(tb))&&e.setAttribute(tb,i.className||""),i.centered&&e.setAttribute(QM,""),i.disabled&&e.setAttribute(Ph,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(Ph,""):e.removeAttribute(Ph)}_onInteraction=e=>{let i=Tt(e);if(i instanceof HTMLElement){let r=i.closest(`[${eb}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(tb)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??kc.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??kc.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(Ph),rippleConfig:{centered:e.hasAttribute(QM),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},l=new Oc(a,this._ngZone,i,this._platform,this._injector),c=!a.rippleDisabled;c&&l.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:l,hasSetUpEvents:c}),e.removeAttribute(eb)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var yi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var o2=["mat-icon-button",""],s2=["*"],a2=new g("MAT_BUTTON_CONFIG");function JM(t){return t==null?void 0:Li(t)}var Fh=(()=>{class t{_elementRef=d(O);_ngZone=d(G);_animationsDisabled=Ge();_config=d(a2,{optional:!0});_focusMonitor=d(wn);_cleanupClick;_renderer=d(Be);_rippleLoader=d(XM);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}constructor(){d(pt).load(yi);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(i,r){i&2&&(le("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),sn(r.color?"mat-"+r.color:""),Z("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",L],disabled:[2,"disabled","disabled",L],ariaDisabled:[2,"aria-disabled","ariaDisabled",L],disabledInteractive:[2,"disabledInteractive","disabledInteractive",L],tabIndex:[2,"tabIndex","tabIndex",JM],_tabindex:[2,"tabindex","_tabindex",JM]}})}return t})(),Nc=(()=>{class t extends Fh{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[oe],attrs:o2,ngContentSelectors:s2,decls:4,vars:0,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(De(),Ot(0,"span",0),H(1),Ot(2,"span",1)(3,"span",2))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--mat-icon-button-state-layer-size, 40px);
  height: var(--mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--mat-icon-button-state-layer-size, 40px) - var(--mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--mat-icon-button-icon-size, 24px);
  color: var(--mat-icon-button-icon-color, var(--mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--mat-icon-button-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface-variant) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-icon-button-touch-target-size, 48px);
  display: var(--mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--mat-icon-button-icon-size, 24px);
  height: var(--mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var ha=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[ye]})}return t})();var l2=["matButton",""],tT=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],nT=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var c2=["mat-mini-fab",""],d2=`.mat-mdc-fab-base {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 56px;
  height: 56px;
  padding: 0;
  border: none;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  -moz-appearance: none;
  -webkit-appearance: none;
  overflow: visible;
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1), opacity 15ms linear 30ms, transform 270ms 0ms cubic-bezier(0, 0, 0.2, 1);
  flex-shrink: 0;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-fab-base .mat-mdc-button-ripple,
.mat-mdc-fab-base .mat-mdc-button-persistent-ripple,
.mat-mdc-fab-base .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-fab-base .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-fab-base .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-fab-base .mdc-button__label,
.mat-mdc-fab-base .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-fab-base .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
}
.mat-mdc-fab-base:focus-visible > .mat-focus-indicator::before {
  content: "";
}
.mat-mdc-fab-base._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-fab-base::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 1px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}
.mat-mdc-fab-base[hidden] {
  display: none;
}
.mat-mdc-fab-base::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mat-mdc-fab-base:active, .mat-mdc-fab-base:focus {
  outline: none;
}
.mat-mdc-fab-base:hover {
  cursor: pointer;
}
.mat-mdc-fab-base > svg {
  width: 100%;
}
.mat-mdc-fab-base .mat-icon, .mat-mdc-fab-base .material-icons {
  transition: transform 180ms 90ms cubic-bezier(0, 0, 0.2, 1);
  fill: currentColor;
  will-change: transform;
}
.mat-mdc-fab-base .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}
.mat-mdc-fab-base[disabled], .mat-mdc-fab-base.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-fab-base[disabled], .mat-mdc-fab-base[disabled]:focus, .mat-mdc-fab-base.mat-mdc-button-disabled, .mat-mdc-fab-base.mat-mdc-button-disabled:focus {
  box-shadow: none;
}
.mat-mdc-fab-base.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-fab {
  background-color: var(--mat-fab-container-color, var(--mat-sys-primary-container));
  border-radius: var(--mat-fab-container-shape, var(--mat-sys-corner-large));
  color: var(--mat-fab-foreground-color, var(--mat-sys-on-primary-container, inherit));
  box-shadow: var(--mat-fab-container-elevation-shadow, var(--mat-sys-level3));
}
@media (hover: hover) {
  .mat-mdc-fab:hover {
    box-shadow: var(--mat-fab-hover-container-elevation-shadow, var(--mat-sys-level4));
  }
}
.mat-mdc-fab:focus {
  box-shadow: var(--mat-fab-focus-container-elevation-shadow, var(--mat-sys-level3));
}
.mat-mdc-fab:active, .mat-mdc-fab:focus:active {
  box-shadow: var(--mat-fab-pressed-container-elevation-shadow, var(--mat-sys-level3));
}
.mat-mdc-fab[disabled], .mat-mdc-fab.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-fab-disabled-state-foreground-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-fab-disabled-state-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-fab.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-fab .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-fab-touch-target-size, 48px);
  display: var(--mat-fab-touch-target-display, block);
  left: 50%;
  width: var(--mat-fab-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-fab .mat-ripple-element {
  background-color: var(--mat-fab-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-fab .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-fab-state-layer-color, var(--mat-sys-on-primary-container));
}
.mat-mdc-fab.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-fab-disabled-state-layer-color);
}
.mat-mdc-fab:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-fab-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-fab.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-fab.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-fab.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-fab-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-fab:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-fab-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}

.mat-mdc-mini-fab {
  width: 40px;
  height: 40px;
  background-color: var(--mat-fab-small-container-color, var(--mat-sys-primary-container));
  border-radius: var(--mat-fab-small-container-shape, var(--mat-sys-corner-medium));
  color: var(--mat-fab-small-foreground-color, var(--mat-sys-on-primary-container, inherit));
  box-shadow: var(--mat-fab-small-container-elevation-shadow, var(--mat-sys-level3));
}
@media (hover: hover) {
  .mat-mdc-mini-fab:hover {
    box-shadow: var(--mat-fab-small-hover-container-elevation-shadow, var(--mat-sys-level4));
  }
}
.mat-mdc-mini-fab:focus {
  box-shadow: var(--mat-fab-small-focus-container-elevation-shadow, var(--mat-sys-level3));
}
.mat-mdc-mini-fab:active, .mat-mdc-mini-fab:focus:active {
  box-shadow: var(--mat-fab-small-pressed-container-elevation-shadow, var(--mat-sys-level3));
}
.mat-mdc-mini-fab[disabled], .mat-mdc-mini-fab.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-fab-small-disabled-state-foreground-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-fab-small-disabled-state-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-mini-fab.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-mini-fab .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-fab-small-touch-target-size, 48px);
  display: var(--mat-fab-small-touch-target-display);
  left: 50%;
  width: var(--mat-fab-small-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-mini-fab .mat-ripple-element {
  background-color: var(--mat-fab-small-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-mini-fab .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-fab-small-state-layer-color, var(--mat-sys-on-primary-container));
}
.mat-mdc-mini-fab.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-fab-small-disabled-state-layer-color);
}
.mat-mdc-mini-fab:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-fab-small-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-mini-fab.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-mini-fab.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-mini-fab.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-fab-small-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-mini-fab:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-fab-small-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}

.mat-mdc-extended-fab {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  padding-left: 20px;
  padding-right: 20px;
  width: auto;
  max-width: 100%;
  line-height: normal;
  box-shadow: var(--mat-fab-extended-container-elevation-shadow, var(--mat-sys-level3));
  height: var(--mat-fab-extended-container-height, 56px);
  border-radius: var(--mat-fab-extended-container-shape, var(--mat-sys-corner-large));
  font-family: var(--mat-fab-extended-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-fab-extended-label-text-size, var(--mat-sys-label-large-size));
  font-weight: var(--mat-fab-extended-label-text-weight, var(--mat-sys-label-large-weight));
  letter-spacing: var(--mat-fab-extended-label-text-tracking, var(--mat-sys-label-large-tracking));
}
@media (hover: hover) {
  .mat-mdc-extended-fab:hover {
    box-shadow: var(--mat-fab-extended-hover-container-elevation-shadow, var(--mat-sys-level4));
  }
}
.mat-mdc-extended-fab:focus {
  box-shadow: var(--mat-fab-extended-focus-container-elevation-shadow, var(--mat-sys-level3));
}
.mat-mdc-extended-fab:active, .mat-mdc-extended-fab:focus:active {
  box-shadow: var(--mat-fab-extended-pressed-container-elevation-shadow, var(--mat-sys-level3));
}
.mat-mdc-extended-fab[disabled], .mat-mdc-extended-fab.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-extended-fab[disabled], .mat-mdc-extended-fab[disabled]:focus, .mat-mdc-extended-fab.mat-mdc-button-disabled, .mat-mdc-extended-fab.mat-mdc-button-disabled:focus {
  box-shadow: none;
}
.mat-mdc-extended-fab.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
[dir=rtl] .mat-mdc-extended-fab .mdc-button__label + .mat-icon, [dir=rtl] .mat-mdc-extended-fab .mdc-button__label + .material-icons,
.mat-mdc-extended-fab > .mat-icon,
.mat-mdc-extended-fab > .material-icons {
  margin-left: -8px;
  margin-right: 12px;
}
.mat-mdc-extended-fab .mdc-button__label + .mat-icon,
.mat-mdc-extended-fab .mdc-button__label + .material-icons, [dir=rtl] .mat-mdc-extended-fab > .mat-icon, [dir=rtl] .mat-mdc-extended-fab > .material-icons {
  margin-left: 12px;
  margin-right: -8px;
}
.mat-mdc-extended-fab .mat-mdc-button-touch-target {
  width: 100%;
}
`,eT=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),bi=(()=>{class t extends Fh{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=u2(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?eT.get(this._appearance):null,o=eT.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[oe],attrs:l2,ngContentSelectors:nT,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(De(tT),Ot(0,"span",0),H(1),lt(2,"span",1),H(3,1),yt(),H(4,2),Ot(5,"span",2)(6,"span",3)),i&2&&Z("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();function u2(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var f2=new g("mat-mdc-fab-default-options",{providedIn:"root",factory:()=>nb}),nb={color:"accent"};var iT=(()=>{class t extends Fh{_options=d(f2,{optional:!0});_isFab=!0;constructor(){super(),this._options=this._options||nb,this.color=this._options.color||nb.color}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["button","mat-mini-fab",""],["a","mat-mini-fab",""],["button","matMiniFab",""],["a","matMiniFab",""]],hostAttrs:[1,"mdc-fab","mat-mdc-fab-base","mdc-fab--mini","mat-mdc-mini-fab"],exportAs:["matButton","matAnchor"],features:[oe],attrs:c2,ngContentSelectors:nT,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(De(tT),Ot(0,"span",0),H(1),lt(2,"span",1),H(3,1),yt(),H(4,2),Ot(5,"span",2)(6,"span",3)),i&2&&Z("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[d2],encapsulation:2,changeDetection:0})}return t})();var zo=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[ha,ye]})}return t})();var rT=(()=>{class t{get vertical(){return this._vertical}set vertical(e){this._vertical=Gi(e)}_vertical=!1;get inset(){return this._inset}set inset(e){this._inset=Gi(e)}_inset=!1;static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-divider"]],hostAttrs:["role","separator",1,"mat-divider"],hostVars:7,hostBindings:function(i,r){i&2&&(le("aria-orientation",r.vertical?"vertical":"horizontal"),Z("mat-divider-vertical",r.vertical)("mat-divider-horizontal",!r.vertical)("mat-divider-inset",r.inset))},inputs:{vertical:"vertical",inset:"inset"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-divider {
  display: block;
  margin: 0;
  border-top-style: solid;
  border-top-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-top-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-vertical {
  border-top: 0;
  border-right-style: solid;
  border-right-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-right-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-inset {
  margin-left: 80px;
}
[dir=rtl] .mat-divider.mat-divider-inset {
  margin-left: auto;
  margin-right: 80px;
}
`],encapsulation:2,changeDetection:0})}return t})(),Vh=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[ye]})}return t})();var hT=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(ne(Be),ne(O))};static \u0275dir=C({type:t})}return t})(),m2=(()=>{class t extends hT{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275dir=C({type:t,features:[oe]})}return t})(),Hc=new g("");var p2={provide:Hc,useExisting:Yt(()=>Nr),multi:!0};function g2(){let t=bn()?bn().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var v2=new g(""),Nr=(()=>{class t extends hT{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!g2())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(ne(Be),ne(O),ne(v2,8))};static \u0275dir=C({type:t,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&J("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[_e([p2]),oe]})}return t})();function sb(t){return t==null||ab(t)===0}function ab(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var $o=new g(""),Zh=new g(""),_2=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,Q=class{static min(n){return y2(n)}static max(n){return b2(n)}static required(n){return mT(n)}static requiredTrue(n){return D2(n)}static email(n){return w2(n)}static minLength(n){return C2(n)}static maxLength(n){return E2(n)}static pattern(n){return x2(n)}static nullValidator(n){return Bh()}static compose(n){return bT(n)}static composeAsync(n){return DT(n)}};function y2(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function b2(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function mT(t){return sb(t.value)?{required:!0}:null}function D2(t){return t.value===!0?null:{required:!0}}function w2(t){return sb(t.value)||_2.test(t.value)?null:{email:!0}}function C2(t){return n=>{let e=n.value?.length??ab(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function E2(t){return n=>{let e=n.value?.length??ab(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function x2(t){if(!t)return Bh;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(sb(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function Bh(t){return null}function pT(t){return t!=null}function gT(t){return _r(t)?He(t):t}function vT(t){let n={};return t.forEach(e=>{n=e!=null?y(y({},n),e):n}),Object.keys(n).length===0?null:n}function _T(t,n){return n.map(e=>e(t))}function S2(t){return!t.validate}function yT(t){return t.map(n=>S2(n)?n:e=>n.validate(e))}function bT(t){if(!t)return null;let n=t.filter(pT);return n.length==0?null:function(e){return vT(_T(e,n))}}function lb(t){return t!=null?bT(yT(t)):null}function DT(t){if(!t)return null;let n=t.filter(pT);return n.length==0?null:function(e){let i=_T(e,n).map(gT);return Ga(i).pipe(fe(vT))}}function cb(t){return t!=null?DT(yT(t)):null}function oT(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function wT(t){return t._rawValidators}function CT(t){return t._rawAsyncValidators}function ib(t){return t?Array.isArray(t)?t:[t]:[]}function Uh(t,n){return Array.isArray(t)?t.includes(n):t===n}function sT(t,n){let e=ib(n);return ib(t).forEach(r=>{Uh(e,r)||e.push(r)}),e}function aT(t,n){return ib(n).filter(e=>!Uh(t,e))}var Hh=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=lb(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=cb(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},Wi=class extends Hh{name;get formDirective(){return null}get path(){return null}},Gn=class extends Hh{_parent=null;name=null;valueAccessor=null},zh=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var Pr=(()=>{class t extends zh{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(ne(Gn,2))};static \u0275dir=C({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&Z("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[oe]})}return t})(),_a=(()=>{class t extends zh{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(ne(Wi,10))};static \u0275dir=C({type:t,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&Z("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[oe]})}return t})();var Pc="VALID",jh="INVALID",ma="PENDING",Fc="DISABLED",Or=class{},$h=class extends Or{value;source;constructor(n,e){super(),this.value=n,this.source=e}},Vc=class extends Or{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},jc=class extends Or{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},pa=class extends Or{status;source;constructor(n,e){super(),this.status=n,this.source=e}},Gh=class extends Or{source;constructor(n){super(),this.source=n}},Uc=class extends Or{source;constructor(n){super(),this.source=n}};function db(t){return(Kh(t)?t.validators:t)||null}function I2(t){return Array.isArray(t)?lb(t):t||null}function ub(t,n){return(Kh(n)?n.asyncValidators:t)||null}function M2(t){return Array.isArray(t)?cb(t):t||null}function Kh(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function ET(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new S(1e3,"");if(!i[e])throw new S(1001,"")}function xT(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new S(-1002,"")})}var ga=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return Te(this.statusReactive)}set status(n){Te(()=>this.statusReactive.set(n))}_status=Pt(()=>this.statusReactive());statusReactive=k(void 0);get valid(){return this.status===Pc}get invalid(){return this.status===jh}get pending(){return this.status===ma}get disabled(){return this.status===Fc}get enabled(){return this.status!==Fc}errors;get pristine(){return Te(this.pristineReactive)}set pristine(n){Te(()=>this.pristineReactive.set(n))}_pristine=Pt(()=>this.pristineReactive());pristineReactive=k(!0);get dirty(){return!this.pristine}get touched(){return Te(this.touchedReactive)}set touched(n){Te(()=>this.touchedReactive.set(n))}_touched=Pt(()=>this.touchedReactive());touchedReactive=k(!1);get untouched(){return!this.touched}_events=new x;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(sT(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(sT(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(aT(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(aT(n,this._rawAsyncValidators))}hasValidator(n){return Uh(this._rawValidators,n)}hasAsyncValidator(n){return Uh(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(te(y({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new jc(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new jc(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(te(y({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new Vc(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new Vc(!0,i))}markAsPending(n={}){this.status=ma;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new pa(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(te(y({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Fc,this.errors=null,this._forEachChild(r=>{r.disable(te(y({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new $h(this.value,i)),this._events.next(new pa(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(te(y({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Pc,this._forEachChild(i=>{i.enable(te(y({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(te(y({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Pc||this.status===ma)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new $h(this.value,e)),this._events.next(new pa(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(te(y({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Fc:Pc}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=ma,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=gT(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new pa(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new j,this.statusChanges=new j}_calculateStatus(){return this._allControlsDisabled()?Fc:this.errors?jh:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(ma)?ma:this._anyControlsHaveStatus(jh)?jh:Pc}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new Vc(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new jc(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Kh(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=I2(this._rawValidators)}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=M2(this._rawAsyncValidators)}},va=class extends ga{constructor(n,e,i){super(db(e),ub(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){return this.controls[n]?this.controls[n]:(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this.controls.hasOwnProperty(n)&&this.controls[n].enabled}setValue(n,e={}){xT(this,!0,n),Object.keys(n).forEach(i=>{ET(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this.controls[i];r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,te(y({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Uc(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return this.controls.hasOwnProperty(n)?this.controls[n]:null}};var rb=class extends va{};var zc=new g("",{factory:()=>Qh}),Qh="always";function ST(t,n){return[...n.path,t]}function Wh(t,n,e=Qh){fb(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),A2(t,n),k2(t,n),R2(t,n),T2(t,n)}function lT(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),Yh(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function qh(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function T2(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function fb(t,n){let e=wT(t);n.validator!==null?t.setValidators(oT(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=CT(t);n.asyncValidator!==null?t.setAsyncValidators(oT(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();qh(n._rawValidators,r),qh(n._rawAsyncValidators,r)}function Yh(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=wT(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=CT(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return qh(n._rawValidators,i),qh(n._rawAsyncValidators,i),e}function A2(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&IT(t,n)})}function R2(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&IT(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function IT(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function k2(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function MT(t,n){t==null,fb(t,n)}function O2(t,n){return Yh(t,n)}function TT(t,n){if(!t.hasOwnProperty("model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function N2(t){return Object.getPrototypeOf(t.constructor)===m2}function AT(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function RT(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===Nr?e=o:N2(o)?i=o:r=o}),r||i||e||null}function P2(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var F2={provide:Wi,useExisting:Yt(()=>$c)},Lc=Promise.resolve(),$c=(()=>{class t extends Wi{callSetDisabledState;get submitted(){return Te(this.submittedReactive)}_submitted=Pt(()=>this.submittedReactive());submittedReactive=k(!1);_directives=new Set;form;ngSubmit=new j;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new va({},lb(e),cb(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){Lc.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),Wh(e.control,e,this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){Lc.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){Lc.then(()=>{let i=this._findContainer(e.path),r=new va({});MT(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){Lc.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){Lc.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),AT(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new Gh(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(ne($o,10),ne(Zh,10),ne(zc,8))};static \u0275dir=C({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&J("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[_e([F2]),oe]})}return t})();function cT(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function dT(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var Bc=class extends ga{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(db(e),ub(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Kh(e)&&(e.nonNullable||e.initialValueIsDefault)&&(dT(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new Uc(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){cT(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){cT(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){dT(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var L2=t=>t instanceof Bc;var V2={provide:Gn,useExisting:Yt(()=>hb)},uT=Promise.resolve(),hb=(()=>{class t extends Gn{_changeDetectorRef;callSetDisabledState;control=new Bc;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new j;constructor(e,i,r,o,s,a){super(),this._changeDetectorRef=s,this.callSetDisabledState=a,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=RT(this,o)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),TT(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){Wh(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){uT.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&L(i);uT.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?ST(e,this._parent):[e]}static \u0275fac=function(i){return new(i||t)(ne(Wi,9),ne($o,10),ne(Zh,10),ne(Hc,10),ne(Pe,8),ne(zc,8))};static \u0275dir=C({type:t,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[_e([V2]),oe,Ie]})}return t})();var ya=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return t})();var ob=class extends ga{constructor(n,e,i){super(db(e),ub(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;at(n){return this.controls[this._adjustIndex(n)]}push(n,e={}){Array.isArray(n)?n.forEach(i=>{this.controls.push(i),this._registerControl(i)}):(this.controls.push(n),this._registerControl(n)),this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}insert(n,e,i={}){this.controls.splice(n,0,e),this._registerControl(e),this.updateValueAndValidity({emitEvent:i.emitEvent})}removeAt(n,e={}){let i=this._adjustIndex(n);i<0&&(i=0),this.controls[i]&&this.controls[i]._registerOnCollectionChange(()=>{}),this.controls.splice(i,1),this.updateValueAndValidity({emitEvent:e.emitEvent})}setControl(n,e,i={}){let r=this._adjustIndex(n);r<0&&(r=0),this.controls[r]&&this.controls[r]._registerOnCollectionChange(()=>{}),this.controls.splice(r,1),e&&(this.controls.splice(r,0,e),this._registerControl(e)),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}get length(){return this.controls.length}setValue(n,e={}){xT(this,!1,n),n.forEach((i,r)=>{ET(this,!1,r),this.at(r).setValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(n.forEach((i,r)=>{this.at(r)&&this.at(r).patchValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n=[],e={}){this._forEachChild((i,r)=>{i.reset(n[r],te(y({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Uc(this))}getRawValue(){return this.controls.map(n=>n.getRawValue())}clear(n={}){this.controls.length<1||(this._forEachChild(e=>e._registerOnCollectionChange(()=>{})),this.controls.splice(0),this.updateValueAndValidity({emitEvent:n.emitEvent}))}_adjustIndex(n){return n<0?n+this.length:n}_syncPendingControls(){let n=this.controls.reduce((e,i)=>i._syncPendingControls()?!0:e,!1);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){this.controls.forEach((e,i)=>{n(e,i)})}_updateValue(){this.value=this.controls.filter(n=>n.enabled||this.disabled).map(n=>n.value)}_anyControls(n){return this.controls.some(e=>e.enabled&&n(e))}_setUpControls(){this._forEachChild(n=>this._registerControl(n))}_allControlsDisabled(){for(let n of this.controls)if(n.enabled)return!1;return this.controls.length>0||this.disabled}_registerControl(n){n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)}_find(n){return this.at(n)??null}};var j2=(()=>{class t extends Wi{callSetDisabledState;get submitted(){return Te(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=Pt(()=>this._submittedReactive());_submittedReactive=k(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Yh(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return Wh(i,e,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){lT(e.control||null,e,!1),P2(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,AT(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new Gh(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(lT(i||null,e),L2(r)&&(Wh(r,e,this.callSetDisabledState),e.control=r))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);MT(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&O2(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){fb(this.form,this),this._oldForm&&Yh(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(ne($o,10),ne(Zh,10),ne(zc,8))};static \u0275dir=C({type:t,features:[oe,Ie]})}return t})();var kT=new g("");var B2={provide:Gn,useExisting:Yt(()=>Go)},Go=(()=>{class t extends Gn{_ngModelWarningConfig;_added=!1;viewModel;control;name=null;set isDisabled(e){}model;update=new j;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(e,i,r,o,s){super(),this._ngModelWarningConfig=s,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=RT(this,o)}ngOnChanges(e){this._added||this._setUpControl(),TT(e,this.viewModel)&&(this.viewModel=this.model,this.formDirective.updateModel(this,this.model))}ngOnDestroy(){this.formDirective?.removeControl(this)}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}get path(){return ST(this.name==null?this.name:this.name.toString(),this._parent)}get formDirective(){return this._parent?this._parent.formDirective:null}_setUpControl(){this.control=this.formDirective.addControl(this),this._added=!0}static \u0275fac=function(i){return new(i||t)(ne(Wi,13),ne($o,10),ne(Zh,10),ne(Hc,10),ne(kT,8))};static \u0275dir=C({type:t,selectors:[["","formControlName",""]],inputs:{name:[0,"formControlName","name"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},standalone:!1,features:[_e([B2]),oe,Ie]})}return t})();var U2={provide:Wi,useExisting:Yt(()=>Wn)},Wn=(()=>{class t extends j2{form=null;ngSubmit=new j;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275dir=C({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&J("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[_e([U2]),oe]})}return t})();var H2=(()=>{class t{_validator=Bh;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):Bh,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,features:[Ie]})}return t})();var z2={provide:$o,useExisting:Yt(()=>Wo),multi:!0};var Wo=(()=>{class t extends H2{required;inputName="required";normalizeInput=L;createValidator=e=>mT;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275dir=C({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&le("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[_e([z2]),oe]})}return t})();var OT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({})}return t})();function fT(t){return!!t&&(t.asyncValidators!==void 0||t.validators!==void 0||t.updateOn!==void 0)}var $2=(()=>{class t{useNonNullable=!1;get nonNullable(){let e=new t;return e.useNonNullable=!0,e}group(e,i=null){let r=this._reduceControls(e),o={};return fT(i)?o=i:i!==null&&(o.validators=i.validator,o.asyncValidators=i.asyncValidator),new va(r,o)}record(e,i=null){let r=this._reduceControls(e);return new rb(r,i)}control(e,i,r){let o={};return this.useNonNullable?(fT(i)?o=i:(o.validators=i,o.asyncValidators=r),new Bc(e,te(y({},o),{nonNullable:!0}))):new Bc(e,i,r)}array(e,i,r){let o=e.map(s=>this._createControl(s));return new ob(o,i,r)}_reduceControls(e){let i={};return Object.keys(e).forEach(r=>{i[r]=this._createControl(e[r])}),i}_createControl(e){if(e instanceof Bc)return e;if(e instanceof ga)return e;if(Array.isArray(e)){let i=e[0],r=e.length>1?e[1]:null,o=e.length>2?e[2]:null;return this.control(i,r,o)}else return this.control(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Xh=(()=>{class t extends $2{group(e,i=null){return super.group(e,i)}control(e,i,r){return super.control(e,i,r)}array(e,i,r){return super.array(e,i,r)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),NT=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:zc,useValue:e.callSetDisabledState??Qh}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[OT]})}return t})(),mb=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:kT,useValue:e.warnOnNgModelWithFormControl??"always"},{provide:zc,useValue:e.callSetDisabledState??Qh}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[OT]})}return t})();var PT=(()=>{class t{_animationsDisabled=Ge();state="unchecked";disabled=!1;appearance="full";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&Z("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--mat-pseudo-checkbox-full-unselected-icon-color, var(--mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--mat-pseudo-checkbox-full-selected-icon-color, var(--mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-full-selected-checkmark-color, var(--mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2,changeDetection:0})}return t})();var G2=["text"],W2=[[["mat-icon"]],"*"],q2=["mat-icon","*"];function Y2(t,n){if(t&1&&he(0,"mat-pseudo-checkbox",1),t&2){let e=V();re("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function Z2(t,n){if(t&1&&he(0,"mat-pseudo-checkbox",3),t&2){let e=V();re("disabled",e.disabled)}}function K2(t,n){if(t&1&&(v(0,"span",4),M(1),_()),t&2){let e=V();D(),$e("(",e.group.label,")")}}var tm=new g("MAT_OPTION_PARENT_COMPONENT"),nm=new g("MatOptgroup");var em=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},qi=(()=>{class t{_element=d(O);_changeDetectorRef=d(Pe);_parent=d(tm,{optional:!0});group=d(nm,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=d(Fe).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=k(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new j;_text;_stateChanges=new x;constructor(){let e=d(pt);e.load(yi),e.load(Th),this._signalDisableRipple=!!this._parent&&di(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!xt(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new em(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&ct(G2,7),i&2){let o;q(o=Y())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&J("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(Gt("id",r.id),le("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),Z("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",L]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:q2,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(De(W2),z(0,Y2,1,2,"mat-pseudo-checkbox",1),H(1),v(2,"span",2,0),H(4,1),_(),z(5,Z2,1,1,"mat-pseudo-checkbox",3),z(6,K2,2,1,"span",4),he(7,"div",5)),i&2&&($(r.multiple?0:-1),D(5),$(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),D(),$(r.group&&r.group._inert?6:-1),D(),re("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[PT,fa],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--mat-option-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-option-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-option-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-option-label-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-option-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-option-label-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--mat-option-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--mat-option-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--mat-option-selected-state-layer-color, var(--mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --mat-list-list-item-selected-container-color: var(--mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2,changeDetection:0})}return t})();function pb(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let s=0;s<t+1;s++)i[s].group&&i[s].group===r[o]&&o++;return o}return 0}function gb(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var vb=class{_box;_destroyed=new x;_resizeSubject=new x;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new de(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(be(e=>e.some(i=>i.target===n)),Ld({bufferSize:1,refCount:!0}),Ae(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},FT=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=d(G);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new vb(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Q2=["notch"],X2=["matFormFieldNotchedOutline",""],J2=["*"],LT=["iconPrefixContainer"],VT=["textPrefixContainer"],jT=["iconSuffixContainer"],BT=["textSuffixContainer"],eU=["textField"],tU=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],nU=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function iU(t,n){t&1&&he(0,"span",21)}function rU(t,n){if(t&1&&(v(0,"label",20),H(1,1),z(2,iU,1,0,"span",21),_()),t&2){let e=V(2);re("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),le("for",e._control.disableAutomaticLabeling?null:e._control.id),D(2),$(!e.hideRequiredMarker&&e._control.required?2:-1)}}function oU(t,n){if(t&1&&z(0,rU,3,5,"label",20),t&2){let e=V();$(e._hasFloatingLabel()?0:-1)}}function sU(t,n){t&1&&he(0,"div",7)}function aU(t,n){}function lU(t,n){if(t&1&&Me(0,aU,0,0,"ng-template",13),t&2){V(2);let e=ui(1);re("ngTemplateOutlet",e)}}function cU(t,n){if(t&1&&(v(0,"div",9),z(1,lU,1,1,null,13),_()),t&2){let e=V();re("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),D(),$(e._forceDisplayInfixLabel()?-1:1)}}function dU(t,n){t&1&&(v(0,"div",10,2),H(2,2),_())}function uU(t,n){t&1&&(v(0,"div",11,3),H(2,3),_())}function fU(t,n){}function hU(t,n){if(t&1&&Me(0,fU,0,0,"ng-template",13),t&2){V();let e=ui(1);re("ngTemplateOutlet",e)}}function mU(t,n){t&1&&(v(0,"div",14,4),H(2,4),_())}function pU(t,n){t&1&&(v(0,"div",15,5),H(2,5),_())}function gU(t,n){t&1&&he(0,"div",16)}function vU(t,n){t&1&&(v(0,"div",18),H(1,6),_())}function _U(t,n){if(t&1&&(v(0,"mat-hint",22),M(1),_()),t&2){let e=V(2);re("id",e._hintLabelId),D(),et(e.hintLabel)}}function yU(t,n){if(t&1&&(v(0,"div",19),z(1,_U,2,2,"mat-hint",22),H(2,7),he(3,"div",23),H(4,8),_()),t&2){let e=V();D(),$(e.hintLabel?1:-1)}}var Di=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["mat-label"]]})}return t})(),qT=new g("MatError"),jr=(()=>{class t{id=d(Fe).getId("mat-mdc-error-");constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["mat-error"],["","matError",""]],hostAttrs:[1,"mat-mdc-form-field-error","mat-mdc-form-field-bottom-align"],hostVars:1,hostBindings:function(i,r){i&2&&Gt("id",r.id)},inputs:{id:"id"},features:[_e([{provide:qT,useExisting:t}])]})}return t})(),Vr=(()=>{class t{align="start";id=d(Fe).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(Gt("id",r.id),le("align",null),Z("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),bU=new g("MatPrefix");var DU=new g("MatSuffix");var YT=new g("FloatingLabelParent"),UT=(()=>{class t{_elementRef=d(O);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=d(FT);_ngZone=d(G);_parent=d(YT);_resizeSubscription=new ae;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return wU(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&Z("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function wU(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var HT="mdc-line-ripple--active",im="mdc-line-ripple--deactivating",zT=(()=>{class t{_elementRef=d(O);_cleanupTransitionEnd;constructor(){let e=d(G),i=d(Be);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(im),e.add(HT)}deactivate(){this._elementRef.nativeElement.classList.add(im)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(im);e.propertyName==="opacity"&&r&&i.remove(HT,im)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),$T=(()=>{class t{_elementRef=d(O);_ngZone=d(G);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&ct(Q2,5),i&2){let o;q(o=Y())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&Z("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:X2,ngContentSelectors:J2,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(De(),Ot(0,"div",1),lt(1,"div",2,0),H(3),yt(),Ot(4,"div",3))},encapsulation:2,changeDetection:0})}return t})(),Gc=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t})}return t})();var Wc=new g("MatFormField"),CU=new g("MAT_FORM_FIELD_DEFAULT_OPTIONS"),GT="fill",EU="auto",WT="fixed",xU="translateY(-50%)",Yi=(()=>{class t{_elementRef=d(O);_changeDetectorRef=d(Pe);_platform=d(Ce);_idGenerator=d(Fe);_ngZone=d(G);_defaults=d(CU,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=jl("iconPrefixContainer");_textPrefixContainerSignal=jl("textPrefixContainer");_iconSuffixContainerSignal=jl("iconSuffixContainer");_textSuffixContainerSignal=jl("textSuffixContainer");_prefixSuffixContainers=Pt(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=fS(Di);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=Gi(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||EU}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||GT;this._appearanceSignal.set(i)}_appearanceSignal=k(GT);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||WT}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||WT}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new x;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Ge();constructor(){let e=this._defaults,i=d(bt);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),kn(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=Pt(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(Je([void 0,void 0]),fe(()=>[i.errorState,i.userAriaDescribedBy]),Fd(),be(([[o,s],[a,l]])=>o!==a||s!==l)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(Ae(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),Lt(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){vS({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=Pt(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(s=>s&&!o.includes(s)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,l=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,u=this._currentDirection==="rtl"?"-1":"1",f=`${s+a}px`,m=`calc(${u} * (${f} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,p=`var(--mat-mdc-form-field-label-transform, ${xU} translateX(${m}))`,w=s+a+l+c;return[p,w]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(pf(o,r._labelChild,Di,5),mt(o,Gc,5)(o,bU,5)(o,DU,5)(o,qT,5)(o,Vr,5)),i&2){vf();let s;q(s=Y())&&(r._formFieldControl=s.first),q(s=Y())&&(r._prefixChildren=s),q(s=Y())&&(r._suffixChildren=s),q(s=Y())&&(r._errorChildren=s),q(s=Y())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(gf(r._iconPrefixContainerSignal,LT,5)(r._textPrefixContainerSignal,VT,5)(r._iconSuffixContainerSignal,jT,5)(r._textSuffixContainerSignal,BT,5),ct(eU,5)(LT,5)(VT,5)(jT,5)(BT,5)(UT,5)($T,5)(zT,5)),i&2){vf(4);let o;q(o=Y())&&(r._textField=o.first),q(o=Y())&&(r._iconPrefixContainer=o.first),q(o=Y())&&(r._textPrefixContainer=o.first),q(o=Y())&&(r._iconSuffixContainer=o.first),q(o=Y())&&(r._textSuffixContainer=o.first),q(o=Y())&&(r._floatingLabel=o.first),q(o=Y())&&(r._notchedOutline=o.first),q(o=Y())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&Z("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[_e([{provide:Wc,useExisting:t},{provide:YT,useExisting:t}])],ngContentSelectors:nU,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(De(tU),Me(0,oU,1,1,"ng-template",null,0,u_),v(2,"div",6,1),J("click",function(s){return r._control.onContainerClick(s)}),z(4,sU,1,0,"div",7),v(5,"div",8),z(6,cU,2,2,"div",9),z(7,dU,3,0,"div",10),z(8,uU,3,0,"div",11),v(9,"div",12),z(10,hU,1,1,null,13),H(11),_(),z(12,mU,3,0,"div",14),z(13,pU,3,0,"div",15),_(),z(14,gU,1,0,"div",16),_(),v(15,"div",17),z(16,vU,2,0,"div",18)(17,yU,5,1,"div",19),_()),i&2){let o;D(2),Z("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),D(2),$(!r._hasOutline()&&!r._control.disabled?4:-1),D(2),$(r._hasOutline()?6:-1),D(),$(r._hasIconPrefix?7:-1),D(),$(r._hasTextPrefix?8:-1),D(2),$(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),D(2),$(r._hasTextSuffix?12:-1),D(),$(r._hasIconSuffix?13:-1),D(),$(r._hasOutline()?-1:14),D(),Z("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();D(),$((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[UT,$T,N_,zT,Vr],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2,changeDetection:0})}return t})();var ZT=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[ye]})}return t})();var _b=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[ha,ZT,qi,ye]})}return t})();var qc=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new x;constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}deselect(...n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}setSelection(...n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var Yc=(()=>{class t{_listeners=[];notify(e,i){for(let r of this._listeners)r(e,i)}listen(e){return this._listeners.push(e),()=>{this._listeners=this._listeners.filter(i=>e!==i)}}ngOnDestroy(){this._listeners=[]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var rm=class{applyChanges(n,e,i,r,o){n.forEachOperation((s,a,l)=>{let c,u;if(s.previousIndex==null){let f=i(s,a,l);c=e.createEmbeddedView(f.templateRef,f.context,f.index),u=zn.INSERTED}else l==null?(e.remove(a),u=zn.REMOVED):(c=e.get(a),e.move(c,l),u=zn.MOVED);o&&o({context:c?.context,operation:u,record:s})})}detach(){}};var SU=["*"];var IU=new g("MAT_CARD_CONFIG"),ba=(()=>{class t{appearance;constructor(){let e=d(IU,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&Z("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:SU,decls:1,vars:0,template:function(i,r){i&1&&(De(),H(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-elevated-container-elevation, var(--mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--mat-card-outlined-container-color, var(--mat-sys-surface));
  border-radius: var(--mat-card-outlined-container-shape, var(--mat-sys-corner-medium));
  border-width: var(--mat-card-outlined-outline-width, 1px);
  border-color: var(--mat-card-outlined-outline-color, var(--mat-sys-outline-variant));
  box-shadow: var(--mat-card-outlined-container-elevation, var(--mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--mat-card-filled-container-color, var(--mat-sys-surface-container-highest));
  border-radius: var(--mat-card-filled-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-filled-container-elevation, var(--mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--mat-card-title-text-font, var(--mat-sys-title-large-font));
  line-height: var(--mat-card-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-size: var(--mat-card-title-text-size, var(--mat-sys-title-large-size));
  letter-spacing: var(--mat-card-title-text-tracking, var(--mat-sys-title-large-tracking));
  font-weight: var(--mat-card-title-text-weight, var(--mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--mat-card-subtitle-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-card-subtitle-text-font, var(--mat-sys-title-medium-font));
  line-height: var(--mat-card-subtitle-text-line-height, var(--mat-sys-title-medium-line-height));
  font-size: var(--mat-card-subtitle-text-size, var(--mat-sys-title-medium-size));
  letter-spacing: var(--mat-card-subtitle-text-tracking, var(--mat-sys-title-medium-tracking));
  font-weight: var(--mat-card-subtitle-text-weight, var(--mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2,changeDetection:0})}return t})(),Da=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["mat-card-title"],["","mat-card-title",""],["","matCardTitle",""]],hostAttrs:[1,"mat-mdc-card-title"]})}return t})();var wa=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return t})();var yb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[ye]})}return t})();var MU=["mat-internal-form-field",""],TU=["*"],QT=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["div","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&Z("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},attrs:MU,ngContentSelectors:TU,decls:1,vars:0,template:function(i,r){i&1&&(De(),H(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2,changeDetection:0})}return t})();var om=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ca=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,e,i,r,o){this._defaultMatcher=n,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var Db=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[ye]})}return t})();var XT=new g("MAT_INPUT_VALUE_ACCESSOR");var wb=new g("CdkAccordion"),JT=(()=>{class t{_stateChanges=new x;_openCloseAllActions=new x;id=d(Fe).getId("cdk-accordion-");multi=!1;openAll(){this.multi&&this._openCloseAllActions.next(!0)}closeAll(){this._openCloseAllActions.next(!1)}ngOnChanges(e){this._stateChanges.next(e)}ngOnDestroy(){this._stateChanges.complete(),this._openCloseAllActions.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["cdk-accordion"],["","cdkAccordion",""]],inputs:{multi:[2,"multi","multi",L]},exportAs:["cdkAccordion"],features:[_e([{provide:wb,useExisting:t}]),Ie]})}return t})(),eA=(()=>{class t{accordion=d(wb,{optional:!0,skipSelf:!0});_changeDetectorRef=d(Pe);_expansionDispatcher=d(Yc);_openCloseAllSubscription=ae.EMPTY;closed=new j;opened=new j;destroyed=new j;expandedChange=new j;id=d(Fe).getId("cdk-accordion-child-");get expanded(){return this._expanded}set expanded(e){if(this._expanded!==e){if(this._expanded=e,this.expandedChange.emit(e),e){this.opened.emit();let i=this.accordion?this.accordion.id:this.id;this._expansionDispatcher.notify(this.id,i)}else this.closed.emit();this._changeDetectorRef.markForCheck()}}_expanded=!1;get disabled(){return this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=k(!1);_removeUniqueSelectionListener=()=>{};constructor(){}ngOnInit(){this._removeUniqueSelectionListener=this._expansionDispatcher.listen((e,i)=>{this.accordion&&!this.accordion.multi&&this.accordion.id===i&&this.id!==e&&(this.expanded=!1)}),this.accordion&&(this._openCloseAllSubscription=this._subscribeToOpenCloseAllActions())}ngOnDestroy(){this.opened.complete(),this.closed.complete(),this.destroyed.emit(),this.destroyed.complete(),this._removeUniqueSelectionListener(),this._openCloseAllSubscription.unsubscribe()}toggle(){this.disabled||(this.expanded=!this.expanded)}close(){this.disabled||(this.expanded=!1)}open(){this.disabled||(this.expanded=!0)}_subscribeToOpenCloseAllActions(){return this.accordion._openCloseAllActions.subscribe(e=>{this.disabled||(this.expanded=e)})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["cdk-accordion-item"],["","cdkAccordionItem",""]],inputs:{expanded:[2,"expanded","expanded",L],disabled:[2,"disabled","disabled",L]},outputs:{closed:"closed",opened:"opened",destroyed:"destroyed",expandedChange:"expandedChange"},exportAs:["cdkAccordionItem"],features:[_e([{provide:wb,useValue:void 0}])]})}return t})(),tA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({})}return t})();var AU=["body"],RU=["bodyWrapper"],kU=[[["mat-expansion-panel-header"]],"*",[["mat-action-row"]]],OU=["mat-expansion-panel-header","*","mat-action-row"];function NU(t,n){}var PU=[[["mat-panel-title"]],[["mat-panel-description"]],"*"],FU=["mat-panel-title","mat-panel-description","*"];function LU(t,n){t&1&&(lt(0,"span",1),oi(),lt(1,"svg",2),Ot(2,"path",3),yt()())}var Cb=new g("MAT_ACCORDION"),nA=new g("MAT_EXPANSION_PANEL"),VU=(()=>{class t{_template=d(je);_expansionPanel=d(nA,{optional:!0});constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["ng-template","matExpansionPanelContent",""]]})}return t})(),iA=new g("MAT_EXPANSION_PANEL_DEFAULT_OPTIONS"),qo=(()=>{class t extends eA{_viewContainerRef=d(Qe);_animationsDisabled=Ge();_document=d(P);_ngZone=d(G);_elementRef=d(O);_renderer=d(Be);_cleanupTransitionEnd;get hideToggle(){return this._hideToggle||this.accordion&&this.accordion.hideToggle}set hideToggle(e){this._hideToggle=e}_hideToggle=!1;get togglePosition(){return this._togglePosition||this.accordion&&this.accordion.togglePosition}set togglePosition(e){this._togglePosition=e}_togglePosition;afterExpand=new j;afterCollapse=new j;_inputChanges=new x;accordion=d(Cb,{optional:!0,skipSelf:!0});_lazyContent;_body;_bodyWrapper;_portal;_headerId=d(Fe).getId("mat-expansion-panel-header-");constructor(){super();let e=d(iA,{optional:!0});this._expansionDispatcher=d(Yc),e&&(this.hideToggle=e.hideToggle)}_hasSpacing(){return this.accordion?this.expanded&&this.accordion.displayMode==="default":!1}_getExpandedState(){return this.expanded?"expanded":"collapsed"}toggle(){this.expanded=!this.expanded}close(){this.expanded=!1}open(){this.expanded=!0}ngAfterContentInit(){this._lazyContent&&this._lazyContent._expansionPanel===this&&this.opened.pipe(Je(null),be(()=>this.expanded&&!this._portal),ze(1)).subscribe(()=>{this._portal=new dn(this._lazyContent._template,this._viewContainerRef)}),this._setupAnimationEvents()}ngOnChanges(e){this._inputChanges.next(e)}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTransitionEnd?.(),this._inputChanges.complete()}_containsFocus(){if(this._body){let e=this._document.activeElement,i=this._body.nativeElement;return e===i||i.contains(e)}return!1}_transitionEndListener=({target:e,propertyName:i})=>{e===this._bodyWrapper?.nativeElement&&i==="grid-template-rows"&&this._ngZone.run(()=>{this.expanded?this.afterExpand.emit():this.afterCollapse.emit()})};_setupAnimationEvents(){this._ngZone.runOutsideAngular(()=>{this._animationsDisabled?(this.opened.subscribe(()=>this._ngZone.run(()=>this.afterExpand.emit())),this.closed.subscribe(()=>this._ngZone.run(()=>this.afterCollapse.emit()))):setTimeout(()=>{let e=this._elementRef.nativeElement;this._cleanupTransitionEnd=this._renderer.listen(e,"transitionend",this._transitionEndListener),e.classList.add("mat-expansion-panel-animations-enabled")},200)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-expansion-panel"]],contentQueries:function(i,r,o){if(i&1&&mt(o,VU,5),i&2){let s;q(s=Y())&&(r._lazyContent=s.first)}},viewQuery:function(i,r){if(i&1&&ct(AU,5)(RU,5),i&2){let o;q(o=Y())&&(r._body=o.first),q(o=Y())&&(r._bodyWrapper=o.first)}},hostAttrs:[1,"mat-expansion-panel"],hostVars:4,hostBindings:function(i,r){i&2&&Z("mat-expanded",r.expanded)("mat-expansion-panel-spacing",r._hasSpacing())},inputs:{hideToggle:[2,"hideToggle","hideToggle",L],togglePosition:"togglePosition"},outputs:{afterExpand:"afterExpand",afterCollapse:"afterCollapse"},exportAs:["matExpansionPanel"],features:[_e([{provide:Cb,useValue:void 0},{provide:nA,useExisting:t}]),oe,Ie],ngContentSelectors:OU,decls:9,vars:4,consts:[["bodyWrapper",""],["body",""],[1,"mat-expansion-panel-content-wrapper"],["role","region",1,"mat-expansion-panel-content",3,"id"],[1,"mat-expansion-panel-body"],[3,"cdkPortalOutlet"]],template:function(i,r){i&1&&(De(kU),H(0),v(1,"div",2,0)(3,"div",3,1)(5,"div",4),H(6,1),Me(7,NU,0,0,"ng-template",5),_(),H(8,2),_()()),i&2&&(D(),le("inert",r.expanded?null:""),D(2),re("id",r.id),le("aria-labelledby",r._headerId),D(4),re("cdkPortalOutlet",r._portal))},dependencies:[gi],styles:[`.mat-expansion-panel {
  box-sizing: content-box;
  display: block;
  margin: 0;
  overflow: hidden;
}
.mat-expansion-panel.mat-expansion-panel-animations-enabled {
  transition: margin 225ms cubic-bezier(0.4, 0, 0.2, 1), box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-expansion-panel {
  position: relative;
  background: var(--mat-expansion-container-background-color, var(--mat-sys-surface));
  color: var(--mat-expansion-container-text-color, var(--mat-sys-on-surface));
  border-radius: var(--mat-expansion-container-shape, 12px);
}
.mat-expansion-panel:not([class*=mat-elevation-z]) {
  box-shadow: var(--mat-expansion-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}
.mat-accordion .mat-expansion-panel:not(.mat-expanded), .mat-accordion .mat-expansion-panel:not(.mat-expansion-panel-spacing) {
  border-radius: 0;
}
.mat-accordion .mat-expansion-panel:first-of-type {
  border-top-right-radius: var(--mat-expansion-container-shape, 12px);
  border-top-left-radius: var(--mat-expansion-container-shape, 12px);
}
.mat-accordion .mat-expansion-panel:last-of-type {
  border-bottom-right-radius: var(--mat-expansion-container-shape, 12px);
  border-bottom-left-radius: var(--mat-expansion-container-shape, 12px);
}
@media (forced-colors: active) {
  .mat-expansion-panel {
    outline: solid 1px;
  }
}

.mat-expansion-panel-content-wrapper {
  display: grid;
  grid-template-rows: 0fr;
  grid-template-columns: 100%;
}
.mat-expansion-panel-animations-enabled .mat-expansion-panel-content-wrapper {
  transition: grid-template-rows 225ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-expansion-panel.mat-expanded > .mat-expansion-panel-content-wrapper {
  grid-template-rows: 1fr;
}
@supports not (grid-template-rows: 0fr) {
  .mat-expansion-panel-content-wrapper {
    height: 0;
  }
  .mat-expansion-panel.mat-expanded > .mat-expansion-panel-content-wrapper {
    height: auto;
  }
}
@media print {
  .mat-expansion-panel-content-wrapper {
    height: 0;
  }
  .mat-expansion-panel.mat-expanded > .mat-expansion-panel-content-wrapper {
    height: auto;
  }
}

.mat-expansion-panel-content {
  display: flex;
  flex-direction: column;
  overflow: visible;
  min-height: 0;
  visibility: hidden;
}
.mat-expansion-panel-animations-enabled .mat-expansion-panel-content {
  transition: visibility 190ms linear;
}
.mat-expansion-panel.mat-expanded > .mat-expansion-panel-content-wrapper > .mat-expansion-panel-content {
  visibility: visible;
}
.mat-expansion-panel-content {
  font-family: var(--mat-expansion-container-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-expansion-container-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-expansion-container-text-weight, var(--mat-sys-body-large-weight));
  line-height: var(--mat-expansion-container-text-line-height, var(--mat-sys-body-large-line-height));
  letter-spacing: var(--mat-expansion-container-text-tracking, var(--mat-sys-body-large-tracking));
}

.mat-expansion-panel-body {
  padding: 0 24px 16px;
}

.mat-expansion-panel-spacing {
  margin: 16px 0;
}
.mat-accordion > .mat-expansion-panel-spacing:first-child, .mat-accordion > *:first-child:not(.mat-expansion-panel) .mat-expansion-panel-spacing {
  margin-top: 0;
}
.mat-accordion > .mat-expansion-panel-spacing:last-child, .mat-accordion > *:last-child:not(.mat-expansion-panel) .mat-expansion-panel-spacing {
  margin-bottom: 0;
}

.mat-action-row {
  border-top-style: solid;
  border-top-width: 1px;
  display: flex;
  flex-direction: row;
  justify-content: flex-end;
  padding: 16px 8px 16px 24px;
  border-top-color: var(--mat-expansion-actions-divider-color, var(--mat-sys-outline));
}
.mat-action-row .mat-button-base,
.mat-action-row .mat-mdc-button-base {
  margin-left: 8px;
}
[dir=rtl] .mat-action-row .mat-button-base,
[dir=rtl] .mat-action-row .mat-mdc-button-base {
  margin-left: 0;
  margin-right: 8px;
}
`],encapsulation:2,changeDetection:0})}return t})(),rA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["mat-action-row"]],hostAttrs:[1,"mat-action-row"]})}return t})(),Yo=(()=>{class t{panel=d(qo,{host:!0});_element=d(O);_focusMonitor=d(wn);_changeDetectorRef=d(Pe);_parentChangeSubscription=ae.EMPTY;constructor(){d(pt).load(yi);let e=this.panel,i=d(iA,{optional:!0}),r=d(new Zt("tabindex"),{optional:!0}),o=e.accordion?e.accordion._stateChanges.pipe(be(s=>!!(s.hideToggle||s.togglePosition))):We;this.tabIndex=parseInt(r||"")||0,this._parentChangeSubscription=Lt(e.opened,e.closed,o,e._inputChanges.pipe(be(s=>!!(s.hideToggle||s.disabled||s.togglePosition)))).subscribe(()=>this._changeDetectorRef.markForCheck()),e.closed.pipe(be(()=>e._containsFocus())).subscribe(()=>this._focusMonitor.focusVia(this._element,"program")),i&&(this.expandedHeight=i.expandedHeight,this.collapsedHeight=i.collapsedHeight)}expandedHeight;collapsedHeight;tabIndex=0;get disabled(){return this.panel.disabled}_toggle(){this.disabled||this.panel.toggle()}_isExpanded(){return this.panel.expanded}_getExpandedState(){return this.panel._getExpandedState()}_getPanelId(){return this.panel.id}_getTogglePosition(){return this.panel.togglePosition}_showToggle(){return!this.panel.hideToggle&&!this.panel.disabled}_getHeaderHeight(){let e=this._isExpanded();return e&&this.expandedHeight?this.expandedHeight:!e&&this.collapsedHeight?this.collapsedHeight:null}_keydown(e){switch(e.keyCode){case 32:case 13:xt(e)||(e.preventDefault(),this._toggle());break;default:this.panel.accordion&&this.panel.accordion._handleHeaderKeydown(e);return}}focus(e,i){e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}ngAfterViewInit(){this._focusMonitor.monitor(this._element).subscribe(e=>{e&&this.panel.accordion&&this.panel.accordion._handleHeaderFocus(this)})}ngOnDestroy(){this._parentChangeSubscription.unsubscribe(),this._focusMonitor.stopMonitoring(this._element)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-expansion-panel-header"]],hostAttrs:["role","button",1,"mat-expansion-panel-header","mat-focus-indicator"],hostVars:13,hostBindings:function(i,r){i&1&&J("click",function(){return r._toggle()})("keydown",function(s){return r._keydown(s)}),i&2&&(le("id",r.panel._headerId)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r._getPanelId())("aria-expanded",r._isExpanded())("aria-disabled",r.panel.disabled),Re("height",r._getHeaderHeight()),Z("mat-expanded",r._isExpanded())("mat-expansion-toggle-indicator-after",r._getTogglePosition()==="after")("mat-expansion-toggle-indicator-before",r._getTogglePosition()==="before"))},inputs:{expandedHeight:"expandedHeight",collapsedHeight:"collapsedHeight",tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:Li(e)]},ngContentSelectors:FU,decls:5,vars:3,consts:[[1,"mat-content"],[1,"mat-expansion-indicator"],["xmlns","http://www.w3.org/2000/svg","viewBox","0 -960 960 960","aria-hidden","true","focusable","false"],["d","M480-345 240-585l56-56 184 184 184-184 56 56-240 240Z"]],template:function(i,r){i&1&&(De(PU),lt(0,"span",0),H(1),H(2,1),H(3,2),yt(),z(4,LU,3,0,"span",1)),i&2&&(Z("mat-content-hide-toggle",!r._showToggle()),D(4),$(r._showToggle()?4:-1))},styles:[`.mat-expansion-panel-header {
  display: flex;
  flex-direction: row;
  align-items: center;
  padding: 0 24px;
  border-radius: inherit;
}
.mat-expansion-panel-animations-enabled .mat-expansion-panel-header {
  transition: height 225ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-expansion-panel-header::before {
  border-radius: inherit;
}
.mat-expansion-panel-header {
  height: var(--mat-expansion-header-collapsed-state-height, 48px);
  font-family: var(--mat-expansion-header-text-font, var(--mat-sys-title-medium-font));
  font-size: var(--mat-expansion-header-text-size, var(--mat-sys-title-medium-size));
  font-weight: var(--mat-expansion-header-text-weight, var(--mat-sys-title-medium-weight));
  line-height: var(--mat-expansion-header-text-line-height, var(--mat-sys-title-medium-line-height));
  letter-spacing: var(--mat-expansion-header-text-tracking, var(--mat-sys-title-medium-tracking));
}
.mat-expansion-panel-header.mat-expanded {
  height: var(--mat-expansion-header-expanded-state-height, 64px);
}
.mat-expansion-panel-header[aria-disabled=true] {
  color: var(--mat-expansion-header-disabled-state-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-expansion-panel-header:not([aria-disabled=true]) {
  cursor: pointer;
}
.mat-expansion-panel:not(.mat-expanded) .mat-expansion-panel-header:not([aria-disabled=true]):hover {
  background: var(--mat-expansion-header-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
@media (hover: none) {
  .mat-expansion-panel:not(.mat-expanded) .mat-expansion-panel-header:not([aria-disabled=true]):hover {
    background: var(--mat-expansion-container-background-color, var(--mat-sys-surface));
  }
}
.mat-expansion-panel .mat-expansion-panel-header:not([aria-disabled=true]).cdk-keyboard-focused, .mat-expansion-panel .mat-expansion-panel-header:not([aria-disabled=true]).cdk-program-focused {
  background: var(--mat-expansion-header-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
}
.mat-expansion-panel-header._mat-animation-noopable {
  transition: none;
}
.mat-expansion-panel-header:focus, .mat-expansion-panel-header:hover {
  outline: none;
}
.mat-expansion-panel-header.mat-expanded:focus, .mat-expansion-panel-header.mat-expanded:hover {
  background: inherit;
}
.mat-expansion-panel-header.mat-expansion-toggle-indicator-before {
  flex-direction: row-reverse;
}
.mat-expansion-panel-header.mat-expansion-toggle-indicator-before .mat-expansion-indicator {
  margin: 0 16px 0 0;
}
[dir=rtl] .mat-expansion-panel-header.mat-expansion-toggle-indicator-before .mat-expansion-indicator {
  margin: 0 0 0 16px;
}

.mat-content {
  display: flex;
  flex: 1;
  flex-direction: row;
  overflow: hidden;
}
.mat-content.mat-content-hide-toggle {
  margin-right: 8px;
}
[dir=rtl] .mat-content.mat-content-hide-toggle {
  margin-right: 0;
  margin-left: 8px;
}
.mat-expansion-toggle-indicator-before .mat-content.mat-content-hide-toggle {
  margin-left: 24px;
  margin-right: 0;
}
[dir=rtl] .mat-expansion-toggle-indicator-before .mat-content.mat-content-hide-toggle {
  margin-right: 24px;
  margin-left: 0;
}

.mat-expansion-panel-header-title {
  color: var(--mat-expansion-header-text-color, var(--mat-sys-on-surface));
}

.mat-expansion-panel-header-title,
.mat-expansion-panel-header-description {
  display: flex;
  flex-grow: 1;
  flex-basis: 0;
  margin-right: 16px;
  align-items: center;
}
[dir=rtl] .mat-expansion-panel-header-title,
[dir=rtl] .mat-expansion-panel-header-description {
  margin-right: 0;
  margin-left: 16px;
}
.mat-expansion-panel-header[aria-disabled=true] .mat-expansion-panel-header-title,
.mat-expansion-panel-header[aria-disabled=true] .mat-expansion-panel-header-description {
  color: inherit;
}

.mat-expansion-panel-header-description {
  flex-grow: 2;
  color: var(--mat-expansion-header-description-color, var(--mat-sys-on-surface-variant));
}

.mat-expansion-panel-animations-enabled .mat-expansion-indicator {
  transition: transform 225ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-expansion-panel-header.mat-expanded .mat-expansion-indicator {
  transform: rotate(180deg);
}
.mat-expansion-indicator::after {
  border-style: solid;
  border-width: 0 2px 2px 0;
  content: "";
  padding: 3px;
  transform: rotate(45deg);
  vertical-align: middle;
  color: var(--mat-expansion-header-indicator-color, var(--mat-sys-on-surface-variant));
  display: var(--mat-expansion-legacy-header-indicator-display, none);
}
.mat-expansion-indicator svg {
  width: 24px;
  height: 24px;
  margin: 0 -8px;
  vertical-align: middle;
  fill: var(--mat-expansion-header-indicator-color, var(--mat-sys-on-surface-variant));
  display: var(--mat-expansion-header-indicator-display, inline-block);
}

@media (forced-colors: active) {
  .mat-expansion-panel-content {
    border-top: 1px solid;
    border-top-left-radius: 0;
    border-top-right-radius: 0;
  }
}
`],encapsulation:2,changeDetection:0})}return t})(),sm=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["mat-panel-description"]],hostAttrs:[1,"mat-expansion-panel-header-description"]})}return t})(),Ea=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["mat-panel-title"]],hostAttrs:[1,"mat-expansion-panel-header-title"]})}return t})(),am=(()=>{class t extends JT{_keyManager;_ownHeaders=new Pn;_headers;hideToggle=!1;displayMode="default";togglePosition="after";ngAfterContentInit(){this._headers.changes.pipe(Je(this._headers)).subscribe(e=>{this._ownHeaders.reset(e.filter(i=>i.panel.accordion===this)),this._ownHeaders.notifyOnChanges()}),this._keyManager=new Uo(this._ownHeaders).withWrap().withHomeAndEnd()}_handleHeaderKeydown(e){this._keyManager.onKeydown(e)}_handleHeaderFocus(e){this._keyManager.updateActiveItem(e)}ngOnDestroy(){super.ngOnDestroy(),this._keyManager?.destroy(),this._ownHeaders.destroy()}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275dir=C({type:t,selectors:[["mat-accordion"]],contentQueries:function(i,r,o){if(i&1&&mt(o,Yo,5),i&2){let s;q(s=Y())&&(r._headers=s)}},hostAttrs:[1,"mat-accordion"],hostVars:2,hostBindings:function(i,r){i&2&&Z("mat-accordion-multi",r.multi)},inputs:{hideToggle:[2,"hideToggle","hideToggle",L],displayMode:"displayMode",togglePosition:"togglePosition"},exportAs:["matAccordion"],features:[_e([{provide:Cb,useExisting:t}]),oe]})}return t})(),Eb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[tA,vi,ye]})}return t})();var Br=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[Rh,Yi,ye]})}return t})();var lm=class{tracker;columnIndex=0;rowIndex=0;get rowCount(){return this.rowIndex+1}get rowspan(){let n=Math.max(...this.tracker);return n>1?this.rowCount+n-1:this.rowCount}positions;update(n,e){this.columnIndex=0,this.rowIndex=0,this.tracker=new Array(n),this.tracker.fill(0,0,this.tracker.length),this.positions=e.map(i=>this._trackTile(i))}_trackTile(n){let e=this._findMatchingGap(n.colspan);return this._markTilePosition(e,n),this.columnIndex=e+n.colspan,new Sb(this.rowIndex,e)}_findMatchingGap(n){n>this.tracker.length;let e=-1,i=-1;do{if(this.columnIndex+n>this.tracker.length){this._nextRow(),e=this.tracker.indexOf(0,this.columnIndex),i=this._findGapEndIndex(e);continue}if(e=this.tracker.indexOf(0,this.columnIndex),e==-1){this._nextRow(),e=this.tracker.indexOf(0,this.columnIndex),i=this._findGapEndIndex(e);continue}i=this._findGapEndIndex(e),this.columnIndex=e+1}while(i-e<n||i==0);return Math.max(e,0)}_nextRow(){this.columnIndex=0,this.rowIndex++;for(let n=0;n<this.tracker.length;n++)this.tracker[n]=Math.max(0,this.tracker[n]-1)}_findGapEndIndex(n){for(let e=n+1;e<this.tracker.length;e++)if(this.tracker[e]!=0)return e;return this.tracker.length}_markTilePosition(n,e){for(let i=0;i<e.colspan;i++)this.tracker[n+i]=e.rowspan}},Sb=class{row;col;constructor(n,e){this.row=n,this.col=e}};var oA=["*"];var UU=`.mat-grid-list {
  display: block;
  position: relative;
}

.mat-grid-tile {
  display: block;
  position: absolute;
  overflow: hidden;
}
.mat-grid-tile .mat-grid-tile-header,
.mat-grid-tile .mat-grid-tile-footer {
  display: flex;
  align-items: center;
  height: 48px;
  color: #fff;
  background: rgba(0, 0, 0, 0.38);
  overflow: hidden;
  padding: 0 16px;
  position: absolute;
  left: 0;
  right: 0;
}
.mat-grid-tile .mat-grid-tile-header > *,
.mat-grid-tile .mat-grid-tile-footer > * {
  margin: 0;
  padding: 0;
  font-weight: normal;
  font-size: inherit;
}
.mat-grid-tile .mat-grid-tile-header.mat-2-line,
.mat-grid-tile .mat-grid-tile-footer.mat-2-line {
  height: 68px;
}
.mat-grid-tile .mat-grid-list-text {
  display: flex;
  flex-direction: column;
  flex: auto;
  box-sizing: border-box;
  overflow: hidden;
}
.mat-grid-tile .mat-grid-list-text > * {
  margin: 0;
  padding: 0;
  font-weight: normal;
  font-size: inherit;
}
.mat-grid-tile .mat-grid-list-text:empty {
  display: none;
}
.mat-grid-tile .mat-grid-tile-header {
  top: 0;
}
.mat-grid-tile .mat-grid-tile-footer {
  bottom: 0;
}
.mat-grid-tile .mat-grid-avatar {
  padding-right: 16px;
}
[dir=rtl] .mat-grid-tile .mat-grid-avatar {
  padding-right: 0;
  padding-left: 16px;
}
.mat-grid-tile .mat-grid-avatar:empty {
  display: none;
}

.mat-grid-tile-header {
  font-size: var(--mat-grid-list-tile-header-primary-text-size, var(--mat-sys-body-large));
}
.mat-grid-tile-header .mat-line {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
  box-sizing: border-box;
}
.mat-grid-tile-header .mat-line:nth-child(n+2) {
  font-size: var(--mat-grid-list-tile-header-secondary-text-size, var(--mat-sys-body-medium));
}

.mat-grid-tile-footer {
  font-size: var(--mat-grid-list-tile-footer-primary-text-size, var(--mat-sys-body-large));
}
.mat-grid-tile-footer .mat-line {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
  box-sizing: border-box;
}
.mat-grid-tile-footer .mat-line:nth-child(n+2) {
  font-size: var(--mat-grid-list-tile-footer-secondary-text-size, var(--mat-sys-body-medium));
}

.mat-grid-tile-content {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  padding: 0;
  margin: 0;
}
`,sA=new g("MAT_GRID_LIST"),HU=(()=>{class t{_element=d(O);_gridList=d(sA,{optional:!0});_rowspan=1;_colspan=1;constructor(){}get rowspan(){return this._rowspan}set rowspan(e){this._rowspan=Math.round(mi(e))}get colspan(){return this._colspan}set colspan(e){this._colspan=Math.round(mi(e))}_setStyle(e,i){this._element.nativeElement.style[e]=i}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-grid-tile"]],hostAttrs:[1,"mat-grid-tile"],hostVars:2,hostBindings:function(i,r){i&2&&le("rowspan",r.rowspan)("colspan",r.colspan)},inputs:{rowspan:"rowspan",colspan:"colspan"},exportAs:["matGridTile"],ngContentSelectors:oA,decls:2,vars:0,consts:[[1,"mat-grid-tile-content"]],template:function(i,r){i&1&&(De(),lt(0,"div",0),H(1),yt())},styles:[`.mat-grid-list {
  display: block;
  position: relative;
}

.mat-grid-tile {
  display: block;
  position: absolute;
  overflow: hidden;
}
.mat-grid-tile .mat-grid-tile-header,
.mat-grid-tile .mat-grid-tile-footer {
  display: flex;
  align-items: center;
  height: 48px;
  color: #fff;
  background: rgba(0, 0, 0, 0.38);
  overflow: hidden;
  padding: 0 16px;
  position: absolute;
  left: 0;
  right: 0;
}
.mat-grid-tile .mat-grid-tile-header > *,
.mat-grid-tile .mat-grid-tile-footer > * {
  margin: 0;
  padding: 0;
  font-weight: normal;
  font-size: inherit;
}
.mat-grid-tile .mat-grid-tile-header.mat-2-line,
.mat-grid-tile .mat-grid-tile-footer.mat-2-line {
  height: 68px;
}
.mat-grid-tile .mat-grid-list-text {
  display: flex;
  flex-direction: column;
  flex: auto;
  box-sizing: border-box;
  overflow: hidden;
}
.mat-grid-tile .mat-grid-list-text > * {
  margin: 0;
  padding: 0;
  font-weight: normal;
  font-size: inherit;
}
.mat-grid-tile .mat-grid-list-text:empty {
  display: none;
}
.mat-grid-tile .mat-grid-tile-header {
  top: 0;
}
.mat-grid-tile .mat-grid-tile-footer {
  bottom: 0;
}
.mat-grid-tile .mat-grid-avatar {
  padding-right: 16px;
}
[dir=rtl] .mat-grid-tile .mat-grid-avatar {
  padding-right: 0;
  padding-left: 16px;
}
.mat-grid-tile .mat-grid-avatar:empty {
  display: none;
}

.mat-grid-tile-header {
  font-size: var(--mat-grid-list-tile-header-primary-text-size, var(--mat-sys-body-large));
}
.mat-grid-tile-header .mat-line {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
  box-sizing: border-box;
}
.mat-grid-tile-header .mat-line:nth-child(n+2) {
  font-size: var(--mat-grid-list-tile-header-secondary-text-size, var(--mat-sys-body-medium));
}

.mat-grid-tile-footer {
  font-size: var(--mat-grid-list-tile-footer-primary-text-size, var(--mat-sys-body-large));
}
.mat-grid-tile-footer .mat-line {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  display: block;
  box-sizing: border-box;
}
.mat-grid-tile-footer .mat-line:nth-child(n+2) {
  font-size: var(--mat-grid-list-tile-footer-secondary-text-size, var(--mat-sys-body-medium));
}

.mat-grid-tile-content {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  padding: 0;
  margin: 0;
}
`],encapsulation:2,changeDetection:0})}return t})();var zU=/^-?\d+((\.\d+)?[A-Za-z%$]?)+$/,Zc=class{_gutterSize;_rows=0;_rowspan=0;_cols;_direction;init(n,e,i,r){this._gutterSize=aA(n),this._rows=e.rowCount,this._rowspan=e.rowspan,this._cols=i,this._direction=r}getBaseTileSize(n,e){return`(${n}% - (${this._gutterSize} * ${e}))`}getTilePosition(n,e){return e===0?"0":Zo(`(${n} + ${this._gutterSize}) * ${e}`)}getTileSize(n,e){return`(${n} * ${e}) + (${e-1} * ${this._gutterSize})`}setStyle(n,e,i){let r=100/this._cols,o=(this._cols-1)/this._cols;this.setColStyles(n,i,r,o),this.setRowStyles(n,e,r,o)}setColStyles(n,e,i,r){let o=this.getBaseTileSize(i,r),s=this._direction==="rtl"?"right":"left";n._setStyle(s,this.getTilePosition(o,e)),n._setStyle("width",Zo(this.getTileSize(o,n.colspan)))}getGutterSpan(){return`${this._gutterSize} * (${this._rowspan} - 1)`}getTileSpan(n){return`${this._rowspan} * ${this.getTileSize(n,1)}`}getComputedHeight(){return null}},Ib=class extends Zc{fixedRowHeight;constructor(n){super(),this.fixedRowHeight=n}init(n,e,i,r){super.init(n,e,i,r),this.fixedRowHeight=aA(this.fixedRowHeight),zU.test(this.fixedRowHeight)}setRowStyles(n,e){n._setStyle("top",this.getTilePosition(this.fixedRowHeight,e)),n._setStyle("height",Zo(this.getTileSize(this.fixedRowHeight,n.rowspan)))}getComputedHeight(){return["height",Zo(`${this.getTileSpan(this.fixedRowHeight)} + ${this.getGutterSpan()}`)]}reset(n){n._setListStyle(["height",null]),n._tiles&&n._tiles.forEach(e=>{e._setStyle("top",null),e._setStyle("height",null)})}},Mb=class extends Zc{rowHeightRatio;baseTileHeight;constructor(n){super(),this._parseRatio(n)}setRowStyles(n,e,i,r){let o=i/this.rowHeightRatio;this.baseTileHeight=this.getBaseTileSize(o,r),n._setStyle("marginTop",this.getTilePosition(this.baseTileHeight,e)),n._setStyle("paddingTop",Zo(this.getTileSize(this.baseTileHeight,n.rowspan)))}getComputedHeight(){return["paddingBottom",Zo(`${this.getTileSpan(this.baseTileHeight)} + ${this.getGutterSpan()}`)]}reset(n){n._setListStyle(["paddingBottom",null]),n._tiles.forEach(e=>{e._setStyle("marginTop",null),e._setStyle("paddingTop",null)})}_parseRatio(n){let e=n.split(":");e.length,this.rowHeightRatio=parseFloat(e[0])/parseFloat(e[1])}},Tb=class extends Zc{setRowStyles(n,e){let i=100/this._rowspan,r=(this._rows-1)/this._rows,o=this.getBaseTileSize(i,r);n._setStyle("top",this.getTilePosition(o,e)),n._setStyle("height",Zo(this.getTileSize(o,n.rowspan)))}reset(n){n._tiles&&n._tiles.forEach(e=>{e._setStyle("top",null),e._setStyle("height",null)})}};function Zo(t){return`calc(${t})`}function aA(t){return t.match(/([A-Za-z%]+)$/)?t:`${t}px`}var $U="fit",xa=(()=>{class t{_element=d(O);_dir=d(bt,{optional:!0});_cols;_tileCoordinator;_rowHeight;_gutter="1px";_tileStyler;_tiles;constructor(){}get cols(){return this._cols}set cols(e){this._cols=Math.max(1,Math.round(mi(e)))}get gutterSize(){return this._gutter}set gutterSize(e){this._gutter=`${e??""}`}get rowHeight(){return this._rowHeight}set rowHeight(e){let i=`${e??""}`;i!==this._rowHeight&&(this._rowHeight=i,this._setTileStyler(this._rowHeight))}ngOnInit(){this._checkCols(),this._checkRowHeight()}ngAfterContentChecked(){this._layoutTiles()}_checkCols(){this.cols}_checkRowHeight(){this._rowHeight||this._setTileStyler("1:1")}_setTileStyler(e){this._tileStyler&&this._tileStyler.reset(this),e===$U?this._tileStyler=new Tb:e&&e.indexOf(":")>-1?this._tileStyler=new Mb(e):this._tileStyler=new Ib(e)}_layoutTiles(){this._tileCoordinator||(this._tileCoordinator=new lm);let e=this._tileCoordinator,i=this._tiles.filter(o=>!o._gridList||o._gridList===this),r=this._dir?this._dir.value:"ltr";this._tileCoordinator.update(this.cols,i),this._tileStyler.init(this.gutterSize,e,this.cols,r),i.forEach((o,s)=>{let a=e.positions[s];this._tileStyler.setStyle(o,a.row,a.col)}),this._setListStyle(this._tileStyler.getComputedHeight())}_setListStyle(e){e&&(this._element.nativeElement.style[e[0]]=e[1])}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-grid-list"]],contentQueries:function(i,r,o){if(i&1&&mt(o,HU,5),i&2){let s;q(s=Y())&&(r._tiles=s)}},hostAttrs:[1,"mat-grid-list"],hostVars:1,hostBindings:function(i,r){i&2&&le("cols",r.cols)},inputs:{cols:"cols",gutterSize:"gutterSize",rowHeight:"rowHeight"},exportAs:["matGridList"],features:[_e([{provide:sA,useExisting:t}])],ngContentSelectors:oA,decls:2,vars:0,template:function(i,r){i&1&&(De(),lt(0,"div"),H(1),yt())},styles:[UU],encapsulation:2,changeDetection:0})}return t})(),Ab=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[Db,ye,Db]})}return t})();function lA(t){return Error(`Unable to find icon with the name "${t}"`)}function GU(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function cA(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function dA(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var Zi=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},fA=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new Zi(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let s=this._sanitizer.sanitize(_t.HTML,r);if(!s)throw dA(r);let a=jo(s);return this._addSvgIconConfig(e,i,new Zi("",a,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new Zi(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(_t.HTML,i);if(!o)throw dA(i);let s=jo(o);return this._addSvgIconSetConfig(e,new Zi("",s,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(_t.RESOURCE_URL,e);if(!i)throw cA(e);let r=this._cachedIconsByUrl.get(i);return r?B(cm(r)):this._loadSvgIconFromConfig(new Zi(e,null)).pipe(Dt(o=>this._cachedIconsByUrl.set(i,o)),fe(o=>cm(o)))}getNamedSvgIcon(e,i=""){let r=uA(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(e,s):$a(lA(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?B(cm(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(fe(i=>cm(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return B(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(ar(a=>{let c=`Loading icon set URL: ${this._sanitizer.sanitize(_t.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(c)),B(null)})));return Ga(o).pipe(fe(()=>{let s=this._extractIconWithNameFromAnySet(e,i);if(!s)throw lA(e);return s}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,e,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(Dt(i=>e.svgText=i),fe(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?B(null):this._fetchIcon(e).pipe(Dt(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(jo("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(jo("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw GU();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(_t.RESOURCE_URL,i);if(!s)throw cA(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let l=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(fe(c=>jo(c)),io(()=>this._inProgressUrlFetches.delete(s)),qa());return this._inProgressUrlFetches.set(s,l),l}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(uA(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return WU(o)?new Zi(o.url,null,o.options):new Zi(o,null)}}static \u0275fac=function(i){return new(i||t)(K(K_,8),K(ql),K(P,8),K(jt))};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function cm(t){return t.cloneNode(!0)}function uA(t,n){return t+":"+n}function WU(t){return!!(t.url&&t.options)}var qU=["*"],YU=new g("MAT_ICON_DEFAULT_OPTIONS"),ZU=new g("mat-icon-location",{providedIn:"root",factory:()=>{let t=d(P),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),hA=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],KU=hA.map(t=>`[${t}]`).join(", "),QU=/^url\(['"]?#(.*?)['"]?\)$/,Sa=(()=>{class t{_elementRef=d(O);_iconRegistry=d(fA);_location=d(ZU);_errorHandler=d(jt);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=ae.EMPTY;constructor(){let e=d(new Zt("aria-hidden"),{optional:!0}),i=d(YU,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${e}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(KU),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)hA.forEach(s=>{let a=i[o],l=a.getAttribute(s),c=l?l.match(QU):null;if(c){let u=r.get(a);u||(u=[],r.set(a,u)),u.push({name:s,value:c[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(ze(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(le("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),sn(r.color?"mat-"+r.color:""),Z("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",L],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:qU,decls:1,vars:0,template:function(i,r){i&1&&(De(),H(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),Kc=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[ye]})}return t})();var XU=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2,changeDetection:0})}return t})(),JU={passive:!0},mA=(()=>{class t{_platform=d(Ce);_ngZone=d(G);_renderer=d(ft).createRenderer(null,null);_styleLoader=d(pt);_monitoredElements=new Map;constructor(){}monitor(e){if(!this._platform.isBrowser)return We;this._styleLoader.load(XU);let i=cn(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new x,s="cdk-text-field-autofilled",a=c=>{c.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!0}))):c.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!1})))},l=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,JU)));return this._monitoredElements.set(i,{subject:o,unlisten:l}),o}stopMonitoring(e){let i=cn(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var pA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({})}return t})();var eH=["button","checkbox","file","hidden","image","radio","range","reset","submit"],tH=new g("MAT_INPUT_CONFIG"),Ia=(()=>{class t{_elementRef=d(O);_platform=d(Ce);ngControl=d(Gn,{optional:!0,self:!0});_autofillMonitor=d(mA);_ngZone=d(G);_formField=d(Wc,{optional:!0});_renderer=d(Be);_uid=d(Fe).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=d(tH,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new x;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=Gi(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(Q.required)??!1}set required(e){this._required=Gi(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&Ky().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=Gi(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>Ky().has(e));constructor(){let e=d($c,{optional:!0}),i=d(Wn,{optional:!0}),r=d(om),o=d(XT,{optional:!0,self:!0}),s=this._elementRef.nativeElement,a=s.nodeName.toLowerCase();o?di(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Ca(r,this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=a==="select",this._isTextarea=a==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&kn(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){eH.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&J("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(Gt("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),le("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),Z("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",L]},exportAs:["matInput"],features:[_e([{provide:Gc,useExisting:t}]),Ie]})}return t})(),Ob=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[Br,Br,pA,ye]})}return t})();var nH=["mat-menu-item",""],iH=[[["mat-icon"],["","matMenuItemIcon",""]],"*"],rH=["mat-icon, [matMenuItemIcon]","*"];function oH(t,n){t&1&&(oi(),v(0,"svg",2),he(1,"polygon",3),_())}var sH=["*"];function aH(t,n){if(t&1){let e=Ct();lt(0,"div",0),mf("click",function(){Ye(e);let r=V();return Ze(r.closed.emit("click"))})("animationstart",function(r){Ye(e);let o=V();return Ze(o._onAnimationStart(r.animationName))})("animationend",function(r){Ye(e);let o=V();return Ze(o._onAnimationDone(r.animationName))})("animationcancel",function(r){Ye(e);let o=V();return Ze(o._onAnimationDone(r.animationName))}),lt(1,"div",1),H(2),yt()()}if(t&2){let e=V();sn(e._classList),Z("mat-menu-panel-animations-disabled",e._animationsDisabled)("mat-menu-panel-exit-animation",e._panelAnimationState==="void")("mat-menu-panel-animating",e._isAnimating()),Gt("id",e.panelId),le("aria-label",e.ariaLabel||null)("aria-labelledby",e.ariaLabelledby||null)("aria-describedby",e.ariaDescribedby||null)}}var Fb=new g("MAT_MENU_PANEL"),Qc=(()=>{class t{_elementRef=d(O);_document=d(P);_focusMonitor=d(wn);_parentMenu=d(Fb,{optional:!0});_changeDetectorRef=d(Pe);role="menuitem";disabled=!1;disableRipple=!1;_hovered=new x;_focused=new x;_highlighted=!1;_triggersSubmenu=!1;constructor(){d(pt).load(yi),this._parentMenu?.addItem?.(this)}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._getHostElement(),e,i):this._getHostElement().focus(i),this._focused.next(this)}ngAfterViewInit(){this._focusMonitor&&this._focusMonitor.monitor(this._elementRef,!1)}ngOnDestroy(){this._focusMonitor&&this._focusMonitor.stopMonitoring(this._elementRef),this._parentMenu&&this._parentMenu.removeItem&&this._parentMenu.removeItem(this),this._hovered.complete(),this._focused.complete()}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._elementRef.nativeElement}_checkDisabled(e){this.disabled&&(e.preventDefault(),e.stopPropagation())}_handleMouseEnter(){this._hovered.next(this)}getLabel(){let e=this._elementRef.nativeElement.cloneNode(!0),i=e.querySelectorAll("mat-icon, .material-icons");for(let r=0;r<i.length;r++)i[r].remove();return e.textContent?.trim()||""}_setHighlighted(e){this._highlighted=e,this._changeDetectorRef.markForCheck()}_setTriggersSubmenu(e){this._triggersSubmenu=e,this._changeDetectorRef.markForCheck()}_hasFocus(){return this._document&&this._document.activeElement===this._getHostElement()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["","mat-menu-item",""]],hostAttrs:[1,"mat-mdc-menu-item","mat-focus-indicator"],hostVars:8,hostBindings:function(i,r){i&1&&J("click",function(s){return r._checkDisabled(s)})("mouseenter",function(){return r._handleMouseEnter()}),i&2&&(le("role",r.role)("tabindex",r._getTabIndex())("aria-disabled",r.disabled)("disabled",r.disabled||null),Z("mat-mdc-menu-item-highlighted",r._highlighted)("mat-mdc-menu-item-submenu-trigger",r._triggersSubmenu))},inputs:{role:"role",disabled:[2,"disabled","disabled",L],disableRipple:[2,"disableRipple","disableRipple",L]},exportAs:["matMenuItem"],attrs:nH,ngContentSelectors:rH,decls:5,vars:3,consts:[[1,"mat-mdc-menu-item-text"],["matRipple","",1,"mat-mdc-menu-ripple",3,"matRippleDisabled","matRippleTrigger"],["viewBox","0 0 5 10","focusable","false","aria-hidden","true",1,"mat-mdc-menu-submenu-icon"],["points","0,0 5,5 0,10"]],template:function(i,r){i&1&&(De(iH),H(0),v(1,"span",0),H(2,1),_(),he(3,"div",1),z(4,oH,2,0,":svg:svg",2)),i&2&&(D(3),re("matRippleDisabled",r.disableRipple||r.disabled)("matRippleTrigger",r._getHostElement()),D(),$(r._triggersSubmenu?4:-1))},dependencies:[fa],encapsulation:2,changeDetection:0})}return t})();var lH=new g("MatMenuContent");var cH=new g("mat-menu-default-options",{providedIn:"root",factory:()=>({overlapTrigger:!1,xPosition:"after",yPosition:"below",backdropClass:"cdk-overlay-transparent-backdrop"})}),Pb="_mat-menu-enter",dm="_mat-menu-exit",Ta=(()=>{class t{_elementRef=d(O);_changeDetectorRef=d(Pe);_injector=d(W);_keyManager;_xPosition;_yPosition;_firstItemFocusRef;_exitFallbackTimeout;_animationsDisabled=Ge();_allItems;_directDescendantItems=new Pn;_classList={};_panelAnimationState="void";_animationDone=new x;_isAnimating=k(!1);parentMenu;direction;overlayPanelClass;backdropClass;ariaLabel;ariaLabelledby;ariaDescribedby;get xPosition(){return this._xPosition}set xPosition(e){this._xPosition=e,this.setPositionClasses()}get yPosition(){return this._yPosition}set yPosition(e){this._yPosition=e,this.setPositionClasses()}templateRef;items;lazyContent;overlapTrigger=!1;hasBackdrop;set panelClass(e){let i=this._previousPanelClass,r=y({},this._classList);i&&i.length&&i.split(" ").forEach(o=>{r[o]=!1}),this._previousPanelClass=e,e&&e.length&&(e.split(" ").forEach(o=>{r[o]=!0}),this._elementRef.nativeElement.className=""),this._classList=r}_previousPanelClass;get classList(){return this.panelClass}set classList(e){this.panelClass=e}closed=new j;close=this.closed;panelId=d(Fe).getId("mat-menu-panel-");constructor(){let e=d(cH);this.overlayPanelClass=e.overlayPanelClass||"",this._xPosition=e.xPosition,this._yPosition=e.yPosition,this.backdropClass=e.backdropClass,this.overlapTrigger=e.overlapTrigger,this.hasBackdrop=e.hasBackdrop}ngOnInit(){this.setPositionClasses()}ngAfterContentInit(){this._updateDirectDescendants(),this._keyManager=new Uo(this._directDescendantItems).withWrap().withTypeAhead().withHomeAndEnd(),this._keyManager.tabOut.subscribe(()=>this.closed.emit("tab")),this._directDescendantItems.changes.pipe(Je(this._directDescendantItems),ot(e=>Lt(...e.map(i=>i._focused)))).subscribe(e=>this._keyManager.updateActiveItem(e)),this._directDescendantItems.changes.subscribe(e=>{let i=this._keyManager;if(this._panelAnimationState==="enter"&&i.activeItem?._hasFocus()){let r=e.toArray(),o=Math.max(0,Math.min(r.length-1,i.activeItemIndex||0));r[o]&&!r[o].disabled?i.setActiveItem(o):i.setNextItemActive()}})}ngOnDestroy(){this._keyManager?.destroy(),this._directDescendantItems.destroy(),this.closed.complete(),this._firstItemFocusRef?.destroy(),clearTimeout(this._exitFallbackTimeout)}_hovered(){return this._directDescendantItems.changes.pipe(Je(this._directDescendantItems),ot(i=>Lt(...i.map(r=>r._hovered))))}addItem(e){}removeItem(e){}_handleKeydown(e){let i=e.keyCode,r=this._keyManager;switch(i){case 27:xt(e)||(e.preventDefault(),this.closed.emit("keydown"));break;case 37:this.parentMenu&&this.direction==="ltr"&&this.closed.emit("keydown");break;case 39:this.parentMenu&&this.direction==="rtl"&&this.closed.emit("keydown");break;default:(i===38||i===40)&&r.setFocusOrigin("keyboard"),r.onKeydown(e);return}}focusFirstItem(e="program"){this._firstItemFocusRef?.destroy(),this._firstItemFocusRef=Ke(()=>{let i=this._resolvePanel();if(!i||!i.contains(document.activeElement)){let r=this._keyManager;r.setFocusOrigin(e).setFirstItemActive(),!r.activeItem&&i&&i.focus()}},{injector:this._injector})}resetActiveItem(){this._keyManager.setActiveItem(-1)}setElevation(e){}setPositionClasses(e=this.xPosition,i=this.yPosition){this._classList=te(y({},this._classList),{"mat-menu-before":e==="before","mat-menu-after":e==="after","mat-menu-above":i==="above","mat-menu-below":i==="below"}),this._changeDetectorRef.markForCheck()}_onAnimationDone(e){let i=e===dm;(i||e===Pb)&&(i&&(clearTimeout(this._exitFallbackTimeout),this._exitFallbackTimeout=void 0),this._animationDone.next(i?"void":"enter"),this._isAnimating.set(!1))}_onAnimationStart(e){(e===Pb||e===dm)&&this._isAnimating.set(!0)}_setIsOpen(e){if(this._panelAnimationState=e?"enter":"void",e){if(this._keyManager.activeItemIndex===0){let i=this._resolvePanel();i&&(i.scrollTop=0)}}else this._animationsDisabled||(this._exitFallbackTimeout=setTimeout(()=>this._onAnimationDone(dm),200));this._animationsDisabled&&setTimeout(()=>{this._onAnimationDone(e?Pb:dm)}),this._changeDetectorRef.markForCheck()}_updateDirectDescendants(){this._allItems.changes.pipe(Je(this._allItems)).subscribe(e=>{this._directDescendantItems.reset(e.filter(i=>i._parentMenu===this)),this._directDescendantItems.notifyOnChanges()})}_resolvePanel(){let e=null;return this._directDescendantItems.length&&(e=this._directDescendantItems.first._getHostElement().closest('[role="menu"]')),e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-menu"]],contentQueries:function(i,r,o){if(i&1&&mt(o,lH,5)(o,Qc,5)(o,Qc,4),i&2){let s;q(s=Y())&&(r.lazyContent=s.first),q(s=Y())&&(r._allItems=s),q(s=Y())&&(r.items=s)}},viewQuery:function(i,r){if(i&1&&ct(je,5),i&2){let o;q(o=Y())&&(r.templateRef=o.first)}},hostVars:3,hostBindings:function(i,r){i&2&&le("aria-label",null)("aria-labelledby",null)("aria-describedby",null)},inputs:{backdropClass:"backdropClass",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],xPosition:"xPosition",yPosition:"yPosition",overlapTrigger:[2,"overlapTrigger","overlapTrigger",L],hasBackdrop:[2,"hasBackdrop","hasBackdrop",e=>e==null?null:L(e)],panelClass:[0,"class","panelClass"],classList:"classList"},outputs:{closed:"closed",close:"close"},exportAs:["matMenu"],features:[_e([{provide:Fb,useExisting:t}])],ngContentSelectors:sH,decls:1,vars:0,consts:[["tabindex","-1","role","menu",1,"mat-mdc-menu-panel",3,"click","animationstart","animationend","animationcancel","id"],[1,"mat-mdc-menu-content"]],template:function(i,r){i&1&&(De(),ff(0,aH,3,12,"ng-template"))},styles:[`mat-menu {
  display: none;
}

.mat-mdc-menu-content {
  margin: 0;
  padding: 8px 0;
  outline: 0;
}
.mat-mdc-menu-content,
.mat-mdc-menu-content .mat-mdc-menu-item .mat-mdc-menu-item-text {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  flex: 1;
  white-space: normal;
  font-family: var(--mat-menu-item-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-menu-item-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-menu-item-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-menu-item-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-menu-item-label-text-weight, var(--mat-sys-label-large-weight));
}

@keyframes _mat-menu-enter {
  from {
    opacity: 0;
    transform: scale(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-menu-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-menu-panel {
  min-width: 112px;
  max-width: 280px;
  overflow: auto;
  box-sizing: border-box;
  outline: 0;
  animation: _mat-menu-enter 120ms cubic-bezier(0, 0, 0.2, 1);
  border-radius: var(--mat-menu-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-menu-container-color, var(--mat-sys-surface-container));
  box-shadow: var(--mat-menu-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
  will-change: transform, opacity;
}
.mat-mdc-menu-panel.mat-menu-panel-exit-animation {
  animation: _mat-menu-exit 100ms 25ms linear forwards;
}
.mat-mdc-menu-panel.mat-menu-panel-animations-disabled {
  animation: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating {
  pointer-events: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating:has(.mat-mdc-menu-content:empty) {
  display: none;
}
@media (forced-colors: active) {
  .mat-mdc-menu-panel {
    outline: solid 1px;
  }
}
.mat-mdc-menu-panel .mat-divider {
  border-top-color: var(--mat-menu-divider-color, var(--mat-sys-surface-variant));
  margin-bottom: var(--mat-menu-divider-bottom-spacing, 8px);
  margin-top: var(--mat-menu-divider-top-spacing, 8px);
}

.mat-mdc-menu-item {
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  cursor: pointer;
  width: 100%;
  text-align: left;
  box-sizing: border-box;
  color: inherit;
  font-size: inherit;
  background: none;
  text-decoration: none;
  margin: 0;
  min-height: 48px;
  padding-left: var(--mat-menu-item-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-trailing-spacing, 12px);
  -webkit-user-select: none;
  user-select: none;
  cursor: pointer;
  outline: none;
  border: none;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-menu-item::-moz-focus-inner {
  border: 0;
}
[dir=rtl] .mat-mdc-menu-item {
  padding-left: var(--mat-menu-item-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-leading-spacing, 12px);
}
.mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-leading-spacing, 12px);
}
.mat-mdc-menu-item, .mat-mdc-menu-item:visited, .mat-mdc-menu-item:link {
  color: var(--mat-menu-item-label-text-color, var(--mat-sys-on-surface));
}
.mat-mdc-menu-item .mat-icon-no-color,
.mat-mdc-menu-item .mat-mdc-menu-submenu-icon {
  color: var(--mat-menu-item-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-menu-item[disabled] {
  cursor: default;
  opacity: 0.38;
}
.mat-mdc-menu-item[disabled]::after {
  display: block;
  position: absolute;
  content: "";
  top: 0;
  left: 0;
  bottom: 0;
  right: 0;
}
.mat-mdc-menu-item:focus {
  outline: 0;
}
.mat-mdc-menu-item .mat-icon {
  flex-shrink: 0;
  margin-right: var(--mat-menu-item-spacing, 12px);
  height: var(--mat-menu-item-icon-size, 24px);
  width: var(--mat-menu-item-icon-size, 24px);
}
[dir=rtl] .mat-mdc-menu-item {
  text-align: right;
}
[dir=rtl] .mat-mdc-menu-item .mat-icon {
  margin-right: 0;
  margin-left: var(--mat-menu-item-spacing, 12px);
}
.mat-mdc-menu-item:not([disabled]):hover {
  background-color: var(--mat-menu-item-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-menu-item:not([disabled]).cdk-program-focused, .mat-mdc-menu-item:not([disabled]).cdk-keyboard-focused, .mat-mdc-menu-item:not([disabled]).mat-mdc-menu-item-highlighted {
  background-color: var(--mat-menu-item-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
}
@media (forced-colors: active) {
  .mat-mdc-menu-item {
    margin-top: 1px;
  }
}

.mat-mdc-menu-submenu-icon {
  width: var(--mat-menu-item-icon-size, 24px);
  height: 10px;
  fill: currentColor;
  padding-left: var(--mat-menu-item-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-submenu-icon {
  padding-right: var(--mat-menu-item-spacing, 12px);
  padding-left: 0;
}
[dir=rtl] .mat-mdc-menu-submenu-icon polygon {
  transform: scaleX(-1);
  transform-origin: center;
}
@media (forced-colors: active) {
  .mat-mdc-menu-submenu-icon {
    fill: CanvasText;
  }
}

.mat-mdc-menu-item .mat-mdc-menu-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
`],encapsulation:2,changeDetection:0})}return t})(),dH=new g("mat-menu-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(W);return()=>Fo(t)}});var Ma=new WeakMap,uH=(()=>{class t{_canHaveBackdrop;_element=d(O);_viewContainerRef=d(Qe);_menuItemInstance=d(Qc,{optional:!0,self:!0});_dir=d(bt,{optional:!0});_focusMonitor=d(wn);_ngZone=d(G);_injector=d(W);_scrollStrategy=d(dH);_changeDetectorRef=d(Pe);_animationsDisabled=Ge();_portal;_overlayRef=null;_menuOpen=!1;_closingActionsSubscription=ae.EMPTY;_menuCloseSubscription=ae.EMPTY;_pendingRemoval;_parentMaterialMenu;_parentInnerPadding;_openedBy=void 0;get _menu(){return this._menuInternal}set _menu(e){e!==this._menuInternal&&(this._menuInternal=e,this._menuCloseSubscription.unsubscribe(),e&&(this._parentMaterialMenu,this._menuCloseSubscription=e.close.subscribe(i=>{this._destroyMenu(i),(i==="click"||i==="tab")&&this._parentMaterialMenu&&this._parentMaterialMenu.closed.emit(i)})),this._menuItemInstance?._setTriggersSubmenu(this._triggersSubmenu()))}_menuInternal=null;constructor(e){this._canHaveBackdrop=e;let i=d(Fb,{optional:!0});this._parentMaterialMenu=i instanceof Ta?i:void 0}ngOnDestroy(){this._menu&&this._ownsMenu(this._menu)&&Ma.delete(this._menu),this._pendingRemoval?.unsubscribe(),this._menuCloseSubscription.unsubscribe(),this._closingActionsSubscription.unsubscribe(),this._overlayRef&&(this._overlayRef.dispose(),this._overlayRef=null)}get menuOpen(){return this._menuOpen}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_triggersSubmenu(){return!!(this._menuItemInstance&&this._parentMaterialMenu&&this._menu)}_closeMenu(){this._menu?.close.emit()}_openMenu(e){if(this._triggerIsAriaDisabled())return;let i=this._menu;if(this._menuOpen||!i)return;this._pendingRemoval?.unsubscribe();let r=Ma.get(i);Ma.set(i,this),r&&r!==this&&r._closeMenu();let o=this._createOverlay(i),s=o.getConfig(),a=s.positionStrategy;this._setPosition(i,a),this._canHaveBackdrop?s.hasBackdrop=i.hasBackdrop==null?!this._triggersSubmenu():i.hasBackdrop:s.hasBackdrop=i.hasBackdrop??!1,o.hasAttached()||(o.attach(this._getPortal(i)),i.lazyContent?.attach(this.menuData)),this._closingActionsSubscription=this._menuClosingActions().subscribe(()=>this._closeMenu()),i.parentMenu=this._triggersSubmenu()?this._parentMaterialMenu:void 0,i.direction=this.dir,e&&i.focusFirstItem(this._openedBy||"program"),this._setIsMenuOpen(!0),i instanceof Ta&&(i._setIsOpen(!0),i._directDescendantItems.changes.pipe(Ae(i.close)).subscribe(()=>{a.withLockedPosition(!1).reapplyLastPosition(),a.withLockedPosition(!0)}))}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}_destroyMenu(e){let i=this._overlayRef,r=this._menu;!i||!this.menuOpen||(this._closingActionsSubscription.unsubscribe(),this._pendingRemoval?.unsubscribe(),r instanceof Ta&&this._ownsMenu(r)?(this._pendingRemoval=r._animationDone.pipe(ze(1)).subscribe(()=>{i.detach(),Ma.has(r)||r.lazyContent?.detach()}),r._setIsOpen(!1)):(i.detach(),r?.lazyContent?.detach()),r&&this._ownsMenu(r)&&Ma.delete(r),this.restoreFocus&&(e==="keydown"||!this._openedBy||!this._triggersSubmenu())&&this.focus(this._openedBy),this._openedBy=void 0,this._setIsMenuOpen(!1))}_setIsMenuOpen(e){e!==this._menuOpen&&(this._menuOpen=e,this._menuOpen?this.menuOpened.emit():this.menuClosed.emit(),this._triggersSubmenu()&&this._menuItemInstance._setHighlighted(e),this._changeDetectorRef.markForCheck())}_createOverlay(e){if(!this._overlayRef){let i=this._getOverlayConfig(e);this._subscribeToPositions(e,i.positionStrategy),this._overlayRef=$i(this._injector,i),this._overlayRef.keydownEvents().subscribe(r=>{this._menu instanceof Ta&&this._menu._handleKeydown(r)})}return this._overlayRef}_getOverlayConfig(e){return new $n({positionStrategy:_c(this._injector,this._getOverlayOrigin()).withLockedPosition().withGrowAfterOpen().withTransformOriginOn(".mat-menu-panel, .mat-mdc-menu-panel"),backdropClass:e.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:e.overlayPanelClass,scrollStrategy:this._scrollStrategy(),direction:this._dir||"ltr",disableAnimations:this._animationsDisabled})}_subscribeToPositions(e,i){e.setPositionClasses&&i.positionChanges.subscribe(r=>{this._ngZone.run(()=>{let o=r.connectionPair.overlayX==="start"?"after":"before",s=r.connectionPair.overlayY==="top"?"below":"above";e.setPositionClasses(o,s)})})}_setPosition(e,i){let[r,o]=e.xPosition==="before"?["end","start"]:["start","end"],[s,a]=e.yPosition==="above"?["bottom","top"]:["top","bottom"],[l,c]=[s,a],[u,f]=[r,o],h=0;if(this._triggersSubmenu()){if(f=r=e.xPosition==="before"?"start":"end",o=u=r==="end"?"start":"end",this._parentMaterialMenu){if(this._parentInnerPadding==null){let m=this._parentMaterialMenu.items.first;this._parentInnerPadding=m?m._getHostElement().offsetTop:0}h=s==="bottom"?this._parentInnerPadding:-this._parentInnerPadding}}else e.overlapTrigger||(l=s==="top"?"bottom":"top",c=a==="top"?"bottom":"top");i.withPositions([{originX:r,originY:l,overlayX:u,overlayY:s,offsetY:h},{originX:o,originY:l,overlayX:f,overlayY:s,offsetY:h},{originX:r,originY:c,overlayX:u,overlayY:a,offsetY:-h},{originX:o,originY:c,overlayX:f,overlayY:a,offsetY:-h}])}_menuClosingActions(){let e=this._getOutsideClickStream(this._overlayRef),i=this._overlayRef.detachments(),r=this._parentMaterialMenu?this._parentMaterialMenu.closed:B(),o=this._parentMaterialMenu?this._parentMaterialMenu._hovered().pipe(be(s=>this._menuOpen&&s!==this._menuItemInstance)):B();return Lt(e,r,o,i)}_getPortal(e){return(!this._portal||this._portal.templateRef!==e.templateRef)&&(this._portal=new dn(e.templateRef,this._viewContainerRef)),this._portal}_ownsMenu(e){return Ma.get(e)===this}_triggerIsAriaDisabled(){return L(this._element.nativeElement.getAttribute("aria-disabled"))}static \u0275fac=function(i){df()};static \u0275dir=C({type:t})}return t})(),gA=(()=>{class t extends uH{_cleanupTouchstart;_hoverSubscription=ae.EMPTY;get _deprecatedMatMenuTriggerFor(){return this.menu}set _deprecatedMatMenuTriggerFor(e){this.menu=e}get menu(){return this._menu}set menu(e){this._menu=e}menuData;restoreFocus=!0;menuOpened=new j;onMenuOpen=this.menuOpened;menuClosed=new j;onMenuClose=this.menuClosed;constructor(){super(!0);let e=d(Be);this._cleanupTouchstart=e.listen(this._element.nativeElement,"touchstart",i=>{Vo(i)||(this._openedBy="touch")},{passive:!0})}triggersSubmenu(){return super._triggersSubmenu()}toggleMenu(){return this.menuOpen?this.closeMenu():this.openMenu()}openMenu(){this._openMenu(!0)}closeMenu(){this._closeMenu()}updatePosition(){this._overlayRef?.updatePosition()}ngAfterContentInit(){this._handleHover()}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTouchstart(),this._hoverSubscription.unsubscribe()}_getOverlayOrigin(){return this._element}_getOutsideClickStream(e){return e.backdropClick()}_handleMousedown(e){Lo(e)||(this._openedBy=e.button===0?"mouse":void 0,this.triggersSubmenu()&&e.preventDefault())}_handleKeydown(e){let i=e.keyCode;(i===13||i===32)&&(this._openedBy="keyboard"),this.triggersSubmenu()&&(i===39&&this.dir==="ltr"||i===37&&this.dir==="rtl")&&(this._openedBy="keyboard",this.openMenu())}_handleClick(e){this.triggersSubmenu()?(e.stopPropagation(),this.openMenu()):this.toggleMenu()}_handleHover(){this.triggersSubmenu()&&this._parentMaterialMenu&&(this._hoverSubscription=this._parentMaterialMenu._hovered().subscribe(e=>{e===this._menuItemInstance&&!e.disabled&&this._parentMaterialMenu?._panelAnimationState!=="void"&&(this._openedBy="mouse",this._openMenu(!1))}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","mat-menu-trigger-for",""],["","matMenuTriggerFor",""]],hostAttrs:[1,"mat-mdc-menu-trigger"],hostVars:3,hostBindings:function(i,r){i&1&&J("click",function(s){return r._handleClick(s)})("mousedown",function(s){return r._handleMousedown(s)})("keydown",function(s){return r._handleKeydown(s)}),i&2&&le("aria-haspopup",r.menu?"menu":null)("aria-expanded",r.menuOpen)("aria-controls",r.menuOpen?r.menu==null?null:r.menu.panelId:null)},inputs:{_deprecatedMatMenuTriggerFor:[0,"mat-menu-trigger-for","_deprecatedMatMenuTriggerFor"],menu:[0,"matMenuTriggerFor","menu"],menuData:[0,"matMenuTriggerData","menuData"],restoreFocus:[0,"matMenuTriggerRestoreFocus","restoreFocus"]},outputs:{menuOpened:"menuOpened",onMenuOpen:"onMenuOpen",menuClosed:"menuClosed",onMenuClose:"onMenuClose"},exportAs:["matMenuTrigger"],features:[oe]})}return t})();var vA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[ha,_i,ye,ra]})}return t})();var hH=["trigger"],mH=["panel"],pH=[[["mat-select-trigger"]],"*"],gH=["mat-select-trigger","*"];function vH(t,n){if(t&1&&(v(0,"span",4),M(1),_()),t&2){let e=V();D(),et(e.placeholder)}}function _H(t,n){t&1&&H(0)}function yH(t,n){if(t&1&&(v(0,"span",11),M(1),_()),t&2){let e=V(2);D(),et(e.triggerValue)}}function bH(t,n){if(t&1&&(v(0,"span",5),z(1,_H,1,0)(2,yH,2,1,"span",11),_()),t&2){let e=V();D(),$(e.customTrigger?1:2)}}function DH(t,n){if(t&1){let e=Ct();v(0,"div",12,1),J("keydown",function(r){Ye(e);let o=V();return Ze(o._handleKeydown(r))}),H(2,1),_()}if(t&2){let e=V();sn(e.panelClass),Z("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",(e._parentFormField==null?null:e._parentFormField.color)==="primary")("mat-accent",(e._parentFormField==null?null:e._parentFormField.color)==="accent")("mat-warn",(e._parentFormField==null?null:e._parentFormField.color)==="warn")("mat-undefined",!(e._parentFormField!=null&&e._parentFormField.color)),le("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var wH=new g("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(W);return()=>Fo(t)}}),CH=new g("MAT_SELECT_CONFIG"),EH=new g("MatSelectTrigger"),Lb=class{source;value;constructor(n,e){this.source=n,this.value=e}},um=(()=>{class t{_viewportRuler=d(pi);_changeDetectorRef=d(Pe);_elementRef=d(O);_dir=d(bt,{optional:!0});_idGenerator=d(Fe);_renderer=d(Be);_parentFormField=d(Wc,{optional:!0});ngControl=d(Gn,{self:!0,optional:!0});_liveAnnouncer=d(Cc);_defaultOptions=d(CH,{optional:!0});_animationsDisabled=Ge();_popoverLocation;_initialized=new x;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=pb(e,this.options,this.optionGroups),s=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=gb(s.offsetTop,s.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new Lb(this,e)}_scrollStrategyFactory=d(wH);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new x;_errorStateTracker;stateChanges=new x;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=k(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(Q.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=xn(()=>{let e=this.options;return e?e.changes.pipe(Je(e),ot(()=>Lt(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(ot(()=>this.optionSelectionChanges))});openedChange=new j;_openedStream=this.openedChange.pipe(be(e=>e),fe(()=>{}));_closedStream=this.openedChange.pipe(be(e=>!e),fe(()=>{}));selectionChange=new j;valueChange=new j;constructor(){let e=d(om),i=d($c,{optional:!0}),r=d(Wn,{optional:!0}),o=d(new Zt("tabindex"),{optional:!0}),s=d(yc,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new Ca(e,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=s?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new qc(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(Ae(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(Ae(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(Je(null),Ae(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete(),this._clearFromModal()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._applyModalPanelOwnership(),this._panelOpen=!0,this._overlayDir.positionChange.pipe(ze(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}_trackedModal=null;_applyModalPanelOwnership(){let e=this._elementRef.nativeElement.closest('body > .cdk-overlay-container [aria-modal="true"]');if(!e)return;let i=`${this.id}-panel`;this._trackedModal&&qy(this._trackedModal,"aria-owns",i),VM(e,"aria-owns",i),this._trackedModal=e}_clearFromModal(){if(!this._trackedModal)return;let e=`${this.id}-panel`;qy(this._trackedModal,"aria-owns",e),this._trackedModal=null}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,s=this._keyManager;if(!s.isTyping()&&o&&!xt(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let a=this.selected;s.onKeydown(e);let l=this.selected;l&&a!==l&&this._liveAnnouncer.announce(l.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,s=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!s&&(r===13||r===32)&&i.activeItem&&!xt(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!s&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let a=this.options.some(l=>!l.disabled&&!l.selected);this.options.forEach(l=>{l.disabled||(a?l.select():l.deselect())})}else{let a=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==a&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!xt(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof sa?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new Tc(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=Lt(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(Ae(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),Lt(...this.options.map(i=>i._stateChanges)).pipe(Ae(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=Tt(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&mt(o,EH,5)(o,qi,5)(o,nm,5),i&2){let s;q(s=Y())&&(r.customTrigger=s.first),q(s=Y())&&(r.options=s),q(s=Y())&&(r.optionGroups=s)}},viewQuery:function(i,r){if(i&1&&ct(hH,5)(mH,5)(Sh,5),i&2){let o;q(o=Y())&&(r.trigger=o.first),q(o=Y())&&(r.panel=o.first),q(o=Y())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&J("keydown",function(s){return r._handleKeydown(s)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(le("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),Z("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",L],disableRipple:[2,"disableRipple","disableRipple",L],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:Li(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",L],placeholder:"placeholder",required:[2,"required","required",L],multiple:[2,"multiple","multiple",L],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",L],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",Li],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",L]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[_e([{provide:Gc,useExisting:t},{provide:tm,useExisting:t}]),Ie],ngContentSelectors:gH,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(De(pH),v(0,"div",2,0),J("click",function(){return r.open()}),v(3,"div",3),z(4,vH,2,1,"span",4)(5,bH,3,1,"span",5),_(),v(6,"div",6)(7,"div",7),oi(),v(8,"svg",8),he(9,"path",9),_()()()(),Me(10,DH,3,16,"ng-template",10),J("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(s){return r._handleOverlayKeydown(s)})),i&2){let o=ui(1);D(3),le("id",r._valueId),D(),$(r.empty?4:5),D(6),re("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[sa,Sh],styles:[`@keyframes _mat-select-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-select-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-select {
  display: inline-block;
  width: 100%;
  outline: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-select-enabled-trigger-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-select-trigger-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-select-trigger-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-select-trigger-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-select-trigger-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-select-trigger-text-tracking, var(--mat-sys-body-large-tracking));
}

div.mat-mdc-select-panel {
  box-shadow: var(--mat-select-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}

.mat-mdc-select-disabled {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-select-disabled .mat-mdc-select-placeholder {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-select-trigger {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  box-sizing: border-box;
  width: 100%;
}
.mat-mdc-select-disabled .mat-mdc-select-trigger {
  -webkit-user-select: none;
  user-select: none;
  cursor: default;
}

.mat-mdc-select-value {
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mat-mdc-select-value-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mat-mdc-select-arrow-wrapper {
  height: 24px;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
}
.mat-form-field-appearance-fill .mdc-text-field--no-label .mat-mdc-select-arrow-wrapper {
  transform: none;
}

.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-invalid .mat-mdc-select-arrow,
.mat-form-field-invalid:not(.mat-form-field-disabled) .mat-mdc-form-field-infix::after {
  color: var(--mat-select-invalid-arrow-color, var(--mat-sys-error));
}

.mat-mdc-select-arrow {
  width: 10px;
  height: 5px;
  position: relative;
  color: var(--mat-select-enabled-arrow-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field.mat-focused .mat-mdc-select-arrow {
  color: var(--mat-select-focused-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-disabled .mat-mdc-select-arrow {
  color: var(--mat-select-disabled-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-select-open .mat-mdc-select-arrow {
  transform: rotate(180deg);
}
.mat-form-field-animations-enabled .mat-mdc-select-arrow {
  transition: transform 80ms linear;
}
.mat-mdc-select-arrow svg {
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}
@media (forced-colors: active) {
  .mat-mdc-select-arrow svg {
    fill: CanvasText;
  }
  .mat-mdc-select-disabled .mat-mdc-select-arrow svg {
    fill: GrayText;
  }
}

div.mat-mdc-select-panel {
  width: 100%;
  max-height: 275px;
  outline: 0;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  transform-origin: top center;
  border-radius: 0 0 4px 4px;
  position: relative;
  background-color: var(--mat-select-panel-background-color, var(--mat-sys-surface-container));
}
.mat-mdc-select-panel-above div.mat-mdc-select-panel {
  border-radius: 4px 4px 0 0;
  transform-origin: bottom center;
}
@media (forced-colors: active) {
  div.mat-mdc-select-panel {
    outline: solid 1px;
  }
}

.mat-select-panel-animations-enabled {
  animation: _mat-select-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-select-panel-animations-enabled.mat-select-panel-exit {
  animation: _mat-select-exit 100ms linear;
}

.mat-mdc-select-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  color: var(--mat-select-placeholder-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field:not(.mat-form-field-animations-enabled) .mat-mdc-select-placeholder, ._mat-animation-noopable .mat-mdc-select-placeholder {
  transition: none;
}
.mat-form-field-hide-placeholder .mat-mdc-select-placeholder {
  color: transparent;
  -webkit-text-fill-color: transparent;
  transition: none;
  display: block;
}

.mat-mdc-form-field-type-mat-select:not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper {
  cursor: pointer;
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mat-mdc-floating-label {
  max-width: calc(100% - 18px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mdc-floating-label--float-above {
  max-width: calc(100% / 0.75 - 24px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-notched-outline__notch {
  max-width: calc(100% - 60px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-text-field--label-floating .mdc-notched-outline__notch {
  max-width: calc(100% - 24px);
}

.mat-mdc-select-min-line:empty::before {
  content: " ";
  white-space: pre;
  width: 1px;
  display: inline-block;
  visibility: hidden;
}

.mat-form-field-appearance-fill .mat-mdc-select-arrow-wrapper {
  transform: var(--mat-select-arrow-transform, translateY(-8px));
}
`],encapsulation:2,changeDetection:0})}return t})();var Vb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[_i,_b,ye,ra,Br,_b]})}return t})();var xH=["switch"],SH=["*"];function IH(t,n){t&1&&(v(0,"span",11),oi(),v(1,"svg",13),he(2,"path",14),_(),v(3,"svg",15),he(4,"path",16),_()())}var MH=new g("mat-slide-toggle-default-options",{providedIn:"root",factory:()=>({disableToggleValue:!1,hideIcon:!1,disabledInteractive:!1})}),fm=class{source;checked;constructor(n,e){this.source=n,this.checked=e}},jb=(()=>{class t{_elementRef=d(O);_focusMonitor=d(wn);_changeDetectorRef=d(Pe);defaults=d(MH);_onChange=e=>{};_onTouched=()=>{};_validatorOnChange=()=>{};_uniqueId;_checked=!1;_createChangeEvent(e){return new fm(this,e)}_labelId;get buttonId(){return`${this.id||this._uniqueId}-button`}_switchElement;focus(){this._switchElement.nativeElement.focus()}_noopAnimations=Ge();_focused=!1;name=null;id;labelPosition="after";ariaLabel=null;ariaLabelledby=null;ariaDescribedby;required=!1;color;disabled=!1;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked=e,this._changeDetectorRef.markForCheck()}hideIcon;disabledInteractive;change=new j;toggleChange=new j;get inputId(){return`${this.id||this._uniqueId}-input`}constructor(){d(pt).load(yi);let e=d(new Zt("tabindex"),{optional:!0}),i=this.defaults;this.tabIndex=e==null?0:parseInt(e)||0,this.color=i.color||"accent",this.id=this._uniqueId=d(Fe).getId("mat-mdc-slide-toggle-"),this.hideIcon=i.hideIcon??!1,this.disabledInteractive=i.disabledInteractive??!1,this._labelId=this._uniqueId+"-label"}ngAfterContentInit(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{e==="keyboard"||e==="program"?(this._focused=!0,this._changeDetectorRef.markForCheck()):e||Promise.resolve().then(()=>{this._focused=!1,this._onTouched(),this._changeDetectorRef.markForCheck()})})}ngOnChanges(e){e.required&&this._validatorOnChange()}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef)}writeValue(e){this.checked=!!e}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorOnChange=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck()}toggle(){this.checked=!this.checked,this._onChange(this.checked)}_emitChangeEvent(){this._onChange(this.checked),this.change.emit(this._createChangeEvent(this.checked))}_handleClick(){this.disabled||(this.toggleChange.emit(),this.defaults.disableToggleValue||(this.checked=!this.checked,this._onChange(this.checked),this.change.emit(new fm(this,this.checked))))}_getAriaLabelledBy(){return this.ariaLabelledby?this.ariaLabelledby:this.ariaLabel?null:this._labelId}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-slide-toggle"]],viewQuery:function(i,r){if(i&1&&ct(xH,5),i&2){let o;q(o=Y())&&(r._switchElement=o.first)}},hostAttrs:[1,"mat-mdc-slide-toggle"],hostVars:13,hostBindings:function(i,r){i&2&&(Gt("id",r.id),le("tabindex",null)("aria-label",null)("name",null)("aria-labelledby",null),sn(r.color?"mat-"+r.color:""),Z("mat-mdc-slide-toggle-focused",r._focused)("mat-mdc-slide-toggle-checked",r.checked)("_mat-animation-noopable",r._noopAnimations))},inputs:{name:"name",id:"id",labelPosition:"labelPosition",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],required:[2,"required","required",L],color:"color",disabled:[2,"disabled","disabled",L],disableRipple:[2,"disableRipple","disableRipple",L],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:Li(e)],checked:[2,"checked","checked",L],hideIcon:[2,"hideIcon","hideIcon",L],disabledInteractive:[2,"disabledInteractive","disabledInteractive",L]},outputs:{change:"change",toggleChange:"toggleChange"},exportAs:["matSlideToggle"],features:[_e([{provide:Hc,useExisting:Yt(()=>t),multi:!0},{provide:$o,useExisting:t,multi:!0}]),Ie],ngContentSelectors:SH,decls:14,vars:27,consts:[["switch",""],["mat-internal-form-field","",3,"labelPosition"],["role","switch","type","button",1,"mdc-switch",3,"click","tabIndex","disabled"],[1,"mat-mdc-slide-toggle-touch-target"],[1,"mdc-switch__track"],[1,"mdc-switch__handle-track"],[1,"mdc-switch__handle"],[1,"mdc-switch__shadow"],[1,"mdc-elevation-overlay"],[1,"mdc-switch__ripple"],["mat-ripple","",1,"mat-mdc-slide-toggle-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-switch__icons"],[1,"mdc-label",3,"click","for"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--on"],["d","M19.69,5.23L8.96,15.96l-4.23-4.23L2.96,13.5l6,6L21.46,7L19.69,5.23z"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--off"],["d","M20 13H4v-2h16v2z"]],template:function(i,r){if(i&1&&(De(),v(0,"div",1)(1,"button",2,0),J("click",function(){return r._handleClick()}),he(3,"div",3)(4,"span",4),v(5,"span",5)(6,"span",6)(7,"span",7),he(8,"span",8),_(),v(9,"span",9),he(10,"span",10),_(),z(11,IH,5,0,"span",11),_()()(),v(12,"label",12),J("click",function(s){return s.stopPropagation()}),H(13),_()()),i&2){let o=ui(2);re("labelPosition",r.labelPosition),D(),Z("mdc-switch--selected",r.checked)("mdc-switch--unselected",!r.checked)("mdc-switch--checked",r.checked)("mdc-switch--disabled",r.disabled)("mat-mdc-slide-toggle-disabled-interactive",r.disabledInteractive),re("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex)("disabled",r.disabled&&!r.disabledInteractive),le("id",r.buttonId)("name",r.name)("aria-label",r.ariaLabel)("aria-labelledby",r._getAriaLabelledBy())("aria-describedby",r.ariaDescribedby)("aria-required",r.required||null)("aria-checked",r.checked)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),D(9),re("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),D(),$(r.hideIcon?-1:11),D(),re("for",r.buttonId),le("id",r._labelId)}},dependencies:[fa,QT],styles:[`.mdc-switch {
  align-items: center;
  background: none;
  border: none;
  cursor: pointer;
  display: inline-flex;
  flex-shrink: 0;
  margin: 0;
  outline: none;
  overflow: visible;
  padding: 0;
  position: relative;
  width: var(--mat-slide-toggle-track-width, 52px);
}
.mdc-switch.mdc-switch--disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-switch.mat-mdc-slide-toggle-disabled-interactive {
  pointer-events: auto;
}

.mdc-switch__track {
  overflow: hidden;
  position: relative;
  width: 100%;
  height: var(--mat-slide-toggle-track-height, 32px);
  border-radius: var(--mat-slide-toggle-track-shape, var(--mat-sys-corner-full));
}
.mdc-switch--disabled.mdc-switch .mdc-switch__track {
  opacity: var(--mat-slide-toggle-disabled-track-opacity, 0.12);
}
.mdc-switch__track::before, .mdc-switch__track::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  width: 100%;
  border-width: var(--mat-slide-toggle-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-track-outline-color, var(--mat-sys-outline));
}
.mdc-switch--selected .mdc-switch__track::before, .mdc-switch--selected .mdc-switch__track::after {
  border-width: var(--mat-slide-toggle-selected-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-selected-track-outline-color, transparent);
}
.mdc-switch--disabled .mdc-switch__track::before, .mdc-switch--disabled .mdc-switch__track::after {
  border-width: var(--mat-slide-toggle-disabled-unselected-track-outline-width, 2px);
  border-color: var(--mat-slide-toggle-disabled-unselected-track-outline-color, var(--mat-sys-on-surface));
}
@media (forced-colors: active) {
  .mdc-switch__track {
    border-color: currentColor;
  }
}
.mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: translateX(0);
  background: var(--mat-slide-toggle-unselected-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch--selected .mdc-switch__track::before {
  transform: translateX(-100%);
}
.mdc-switch--selected .mdc-switch__track::before {
  opacity: var(--mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::before {
  opacity: var(--mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-hover-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-focus-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch:enabled:active .mdc-switch__track::before {
  background: var(--mat-slide-toggle-unselected-pressed-track-color, var(--mat-sys-surface-variant));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__track::before, .mdc-switch.mdc-switch--disabled .mdc-switch__track::before {
  background: var(--mat-slide-toggle-disabled-unselected-track-color, var(--mat-sys-surface-variant));
}
.mdc-switch__track::after {
  transform: translateX(-100%);
  background: var(--mat-slide-toggle-selected-track-color, var(--mat-sys-primary));
}
[dir=rtl] .mdc-switch__track::after {
  transform: translateX(100%);
}
.mdc-switch--selected .mdc-switch__track::after {
  transform: translateX(0);
}
.mdc-switch--selected .mdc-switch__track::after {
  opacity: var(--mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::after {
  opacity: var(--mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-hover-track-color, var(--mat-sys-primary));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-focus-track-color, var(--mat-sys-primary));
}
.mdc-switch:enabled:active .mdc-switch__track::after {
  background: var(--mat-slide-toggle-selected-pressed-track-color, var(--mat-sys-primary));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__track::after, .mdc-switch.mdc-switch--disabled .mdc-switch__track::after {
  background: var(--mat-slide-toggle-disabled-selected-track-color, var(--mat-sys-on-surface));
}

.mdc-switch__handle-track {
  height: 100%;
  pointer-events: none;
  position: absolute;
  top: 0;
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  left: 0;
  right: auto;
  transform: translateX(0);
  width: calc(100% - var(--mat-slide-toggle-handle-width));
}
[dir=rtl] .mdc-switch__handle-track {
  left: auto;
  right: 0;
}
.mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(-100%);
}

.mdc-switch__handle {
  display: flex;
  pointer-events: auto;
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
  left: 0;
  right: auto;
  transition: width 75ms cubic-bezier(0.4, 0, 0.2, 1), height 75ms cubic-bezier(0.4, 0, 0.2, 1), margin 75ms cubic-bezier(0.4, 0, 0.2, 1);
  width: var(--mat-slide-toggle-handle-width);
  height: var(--mat-slide-toggle-handle-height);
  border-radius: var(--mat-slide-toggle-handle-shape, var(--mat-sys-corner-full));
}
[dir=rtl] .mdc-switch__handle {
  left: auto;
  right: 0;
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle {
  width: var(--mat-slide-toggle-unselected-handle-size, 16px);
  height: var(--mat-slide-toggle-unselected-handle-size, 16px);
  margin: var(--mat-slide-toggle-unselected-handle-horizontal-margin, 0 8px);
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--mat-slide-toggle-unselected-with-icon-handle-horizontal-margin, 0 4px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle {
  width: var(--mat-slide-toggle-selected-handle-size, 24px);
  height: var(--mat-slide-toggle-selected-handle-size, 24px);
  margin: var(--mat-slide-toggle-selected-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--mat-slide-toggle-selected-with-icon-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch__handle:has(.mdc-switch__icons) {
  width: var(--mat-slide-toggle-with-icon-handle-size, 24px);
  height: var(--mat-slide-toggle-with-icon-handle-size, 24px);
}
.mat-mdc-slide-toggle .mdc-switch:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  width: var(--mat-slide-toggle-pressed-handle-size, 28px);
  height: var(--mat-slide-toggle-pressed-handle-size, 28px);
}
.mat-mdc-slide-toggle .mdc-switch--selected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--mat-slide-toggle-selected-pressed-handle-horizontal-margin, 0 22px);
}
.mat-mdc-slide-toggle .mdc-switch--unselected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--mat-slide-toggle-unselected-pressed-handle-horizontal-margin, 0 2px);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__handle::after {
  opacity: var(--mat-slide-toggle-disabled-selected-handle-opacity, 1);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__handle::after {
  opacity: var(--mat-slide-toggle-disabled-unselected-handle-opacity, 0.38);
}
.mdc-switch__handle::before, .mdc-switch__handle::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  width: 100%;
  height: 100%;
  left: 0;
  position: absolute;
  top: 0;
  transition: background-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1), border-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  z-index: -1;
}
@media (forced-colors: active) {
  .mdc-switch__handle::before, .mdc-switch__handle::after {
    border-color: currentColor;
  }
}
.mdc-switch--selected:enabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-handle-color, var(--mat-sys-on-primary));
}
.mdc-switch--selected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-hover-handle-color, var(--mat-sys-primary-container));
}
.mdc-switch--selected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-focus-handle-color, var(--mat-sys-primary-container));
}
.mdc-switch--selected:enabled:active .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-selected-pressed-handle-color, var(--mat-sys-primary-container));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:hover:not(:focus):not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:focus:not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--selected:active .mdc-switch__handle::after, .mdc-switch--selected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-disabled-selected-handle-color, var(--mat-sys-surface));
}
.mdc-switch--unselected:enabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-handle-color, var(--mat-sys-outline));
}
.mdc-switch--unselected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-hover-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-focus-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected:enabled:active .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-unselected-pressed-handle-color, var(--mat-sys-on-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--mat-slide-toggle-disabled-unselected-handle-color, var(--mat-sys-on-surface));
}
.mdc-switch__handle::before {
  background: var(--mat-slide-toggle-handle-surface-color);
}

.mdc-switch__shadow {
  border-radius: inherit;
  bottom: 0;
  left: 0;
  position: absolute;
  right: 0;
  top: 0;
}
.mdc-switch:enabled .mdc-switch__shadow {
  box-shadow: var(--mat-slide-toggle-handle-elevation-shadow);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:hover:not(:focus):not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:focus:not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:active .mdc-switch__shadow, .mdc-switch.mdc-switch--disabled .mdc-switch__shadow {
  box-shadow: var(--mat-slide-toggle-disabled-handle-elevation-shadow);
}

.mdc-switch__ripple {
  left: 50%;
  position: absolute;
  top: 50%;
  transform: translate(-50%, -50%);
  z-index: -1;
  width: var(--mat-slide-toggle-state-layer-size, 40px);
  height: var(--mat-slide-toggle-state-layer-size, 40px);
}
.mdc-switch__ripple::after {
  content: "";
  opacity: 0;
}
.mdc-switch--disabled .mdc-switch__ripple::after {
  display: none;
}
.mat-mdc-slide-toggle-disabled-interactive .mdc-switch__ripple::after {
  display: block;
}
.mdc-switch:hover .mdc-switch__ripple::after {
  transition: 75ms opacity cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:focus .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:active .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled:enabled:hover:not(:focus) .mdc-switch__ripple::after, .mdc-switch--unselected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mdc-switch--unselected:enabled:focus .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mdc-switch--unselected:enabled:active .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-unselected-pressed-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-slide-toggle-unselected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}
.mdc-switch--selected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-hover-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mdc-switch--selected:enabled:focus .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-focus-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mdc-switch--selected:enabled:active .mdc-switch__ripple::after {
  background: var(--mat-slide-toggle-selected-pressed-state-layer-color, var(--mat-sys-primary));
  opacity: var(--mat-slide-toggle-selected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}

.mdc-switch__icons {
  position: relative;
  height: 100%;
  width: 100%;
  z-index: 1;
  transform: translateZ(0);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__icons {
  opacity: var(--mat-slide-toggle-disabled-unselected-icon-opacity, 0.38);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__icons {
  opacity: var(--mat-slide-toggle-disabled-selected-icon-opacity, 0.38);
}

.mdc-switch__icon {
  bottom: 0;
  left: 0;
  margin: auto;
  position: absolute;
  right: 0;
  top: 0;
  opacity: 0;
  transition: opacity 30ms 0ms cubic-bezier(0.4, 0, 1, 1);
}
.mdc-switch--unselected .mdc-switch__icon {
  width: var(--mat-slide-toggle-unselected-icon-size, 16px);
  height: var(--mat-slide-toggle-unselected-icon-size, 16px);
  fill: var(--mat-slide-toggle-unselected-icon-color, var(--mat-sys-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--mat-slide-toggle-disabled-unselected-icon-color, var(--mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__icon {
  width: var(--mat-slide-toggle-selected-icon-size, 16px);
  height: var(--mat-slide-toggle-selected-icon-size, 16px);
  fill: var(--mat-slide-toggle-selected-icon-color, var(--mat-sys-on-primary-container));
}
.mdc-switch--selected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--mat-slide-toggle-disabled-selected-icon-color, var(--mat-sys-on-surface));
}

.mdc-switch--selected .mdc-switch__icon--on,
.mdc-switch--unselected .mdc-switch__icon--off {
  opacity: 1;
  transition: opacity 45ms 30ms cubic-bezier(0, 0, 0.2, 1);
}

.mat-mdc-slide-toggle {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  -webkit-tap-highlight-color: transparent;
  outline: 0;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple,
.mat-mdc-slide-toggle .mdc-switch__ripple::after {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple:not(:empty),
.mat-mdc-slide-toggle .mdc-switch__ripple::after:not(:empty) {
  transform: translateZ(0);
}
.mat-mdc-slide-toggle.mat-mdc-slide-toggle-focused .mat-focus-indicator::before {
  content: "";
}
.mat-mdc-slide-toggle .mat-internal-form-field {
  color: var(--mat-slide-toggle-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-slide-toggle-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-slide-toggle-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-slide-toggle-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-slide-toggle-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-slide-toggle-label-text-weight, var(--mat-sys-body-medium-weight));
}
.mat-mdc-slide-toggle .mat-ripple-element {
  opacity: 0.12;
}
.mat-mdc-slide-toggle .mat-focus-indicator::before {
  border-radius: 50%;
}
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle-track,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__icon,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::after,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::after {
  transition: none;
}
.mat-mdc-slide-toggle .mdc-switch:enabled + .mdc-label {
  cursor: pointer;
}
.mat-mdc-slide-toggle .mdc-switch--disabled + label {
  color: var(--mat-slide-toggle-disabled-label-text-color, var(--mat-sys-on-surface));
}
.mat-mdc-slide-toggle label:empty {
  display: none;
}

.mat-mdc-slide-toggle-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-slide-toggle-touch-target-size, 48px);
  width: 100%;
  transform: translate(-50%, -50%);
  display: var(--mat-slide-toggle-touch-target-display, block);
}
[dir=rtl] .mat-mdc-slide-toggle-touch-target {
  left: auto;
  right: 50%;
  transform: translate(50%, -50%);
}
`],encapsulation:2,changeDetection:0})}return t})(),yA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[jb,ye]})}return t})();function AH(t,n){if(t&1){let e=Ct();v(0,"div",1)(1,"button",2),J("click",function(){Ye(e);let r=V();return Ze(r.action())}),M(2),_()()}if(t&2){let e=V();D(2),$e(" ",e.data.action," ")}}var RH=["label"];function kH(t,n){}var OH=Math.pow(2,31)-1,Xc=class{_overlayRef;instance;containerInstance;_afterDismissed=new x;_afterOpened=new x;_onAction=new x;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,OH))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},bA=new g("MatSnackBarData"),Ur=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},NH=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),PH=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),FH=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),LH=(()=>{class t{snackBarRef=d(Xc);data=d(bA);constructor(){}action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(v(0,"div",0),M(1),_(),z(2,AH,3,1,"div",1)),i&2&&(D(),$e(" ",r.data.message,`
`),D(),$(r.hasAction?2:-1))},dependencies:[bi,NH,PH,FH],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),Bb="_mat-snack-bar-enter",Ub="_mat-snack-bar-exit",VH=(()=>{class t extends Tr{_ngZone=d(G);_elementRef=d(O);_changeDetectorRef=d(Pe);_platform=d(Ce);_animationsDisabled=Ge();snackBarConfig=d(Ur);_document=d(P);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=d(W);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new x;_onExit=new x;_onEnter=new x;_animationState="void";_live;_label;_role;_liveElementId=d(Fe).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===Ub?this._completeExit():e===Bb&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?Ke(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(Bb)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(Bb)},200)))}exit(){return this._destroyed?B(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?Ke(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(Ub)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(Ub),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(s=>e.classList.add(s)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");this._trackedModals.add(o),s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&ct(gi,7)(RH,7),i&2){let o;q(o=Y())&&(r._portalOutlet=o.first),q(o=Y())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&J("animationend",function(s){return r.onAnimationEnd(s.animationName)})("animationcancel",function(s){return r.onAnimationEnd(s.animationName)}),i&2&&Z("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[oe],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(v(0,"div",1)(1,"div",2,0)(3,"div",3),Me(4,kH,0,0,"ng-template",4),_(),he(5,"div"),_()()),i&2&&(D(5),le("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[gi],styles:[`@keyframes _mat-snack-bar-enter {
  from {
    transform: scale(0.8);
    opacity: 0;
  }
  to {
    transform: scale(1);
    opacity: 1;
  }
}
@keyframes _mat-snack-bar-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-snack-bar-container {
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
  margin: 8px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snack-bar-container {
  width: 100vw;
}

.mat-snack-bar-container-animations-enabled {
  opacity: 0;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-fallback-visible {
  opacity: 1;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-enter {
  animation: _mat-snack-bar-enter 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-exit {
  animation: _mat-snack-bar-exit 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}

.mat-mdc-snackbar-surface {
  box-shadow: 0px 3px 5px -1px rgba(0, 0, 0, 0.2), 0px 6px 10px 0px rgba(0, 0, 0, 0.14), 0px 1px 18px 0px rgba(0, 0, 0, 0.12);
  display: flex;
  align-items: center;
  justify-content: flex-start;
  box-sizing: border-box;
  padding-left: 0;
  padding-right: 8px;
}
[dir=rtl] .mat-mdc-snackbar-surface {
  padding-right: 0;
  padding-left: 8px;
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  min-width: 344px;
  max-width: 672px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snackbar-surface {
  width: 100%;
  min-width: 0;
}
@media (forced-colors: active) {
  .mat-mdc-snackbar-surface {
    outline: solid 1px;
  }
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  color: var(--mat-snack-bar-supporting-text-color, var(--mat-sys-inverse-on-surface));
  border-radius: var(--mat-snack-bar-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-snack-bar-container-color, var(--mat-sys-inverse-surface));
}

.mdc-snackbar__label {
  width: 100%;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  padding: 14px 8px 14px 16px;
}
[dir=rtl] .mdc-snackbar__label {
  padding-left: 8px;
  padding-right: 16px;
}
.mat-mdc-snack-bar-container .mdc-snackbar__label {
  font-family: var(--mat-snack-bar-supporting-text-font, var(--mat-sys-body-medium-font));
  font-size: var(--mat-snack-bar-supporting-text-size, var(--mat-sys-body-medium-size));
  font-weight: var(--mat-snack-bar-supporting-text-weight, var(--mat-sys-body-medium-weight));
  line-height: var(--mat-snack-bar-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
}

.mat-mdc-snack-bar-actions {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  box-sizing: border-box;
}

.mat-mdc-snack-bar-handset,
.mat-mdc-snack-bar-container,
.mat-mdc-snack-bar-label {
  flex: 1 1 auto;
}

.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled).mat-unthemed {
  color: var(--mat-snack-bar-button-color, var(--mat-sys-inverse-primary));
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) {
  --mat-button-text-state-layer-color: currentColor;
  --mat-button-text-ripple-color: currentColor;
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) .mat-ripple-element {
  opacity: 0.1;
}
`],encapsulation:2})}return t})(),jH=new g("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new Ur}),Hr=(()=>{class t{_live=d(Cc);_injector=d(W);_breakpointObserver=d(wc);_parentSnackBar=d(t,{optional:!0,skipSelf:!0});_defaultConfig=d(jH);_animationsDisabled=Ge();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=LH;snackBarContainerComponent=VH;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}constructor(){}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=y(y({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=W.create({parent:r||this._injector,providers:[{provide:Ur,useValue:i}]}),s=new zi(this.snackBarContainerComponent,i.viewContainerRef,o),a=e.attach(s);return a.instance.snackBarConfig=i,a.instance}_attach(e,i){let r=y(y(y({},new Ur),this._defaultConfig),i),o=this._createOverlay(r),s=this._attachSnackBarContainer(o,r),a=new Xc(s,o);if(e instanceof je){let l=new dn(e,null,{$implicit:r.data,snackBarRef:a});a.instance=s.attachTemplatePortal(l)}else{let l=this._createInjector(r,a),c=new zi(e,void 0,l),u=s.attachComponentPortal(c);a.instance=u.instance}return this._breakpointObserver.observe(NM.HandsetPortrait).pipe(Ae(o.detachments())).subscribe(l=>{o.overlayElement.classList.toggle(this.handsetCssClass,l.matches)}),r.announcementMessage&&s._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(a,r),this._openedSnackBarRef=a,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new $n;i.direction=e.direction;let r=Ar(this._injector),o=e.direction==="rtl",s=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,a=!s&&e.horizontalPosition!=="center";return s?r.left("0"):a?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,$i(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return W.create({parent:r||this._injector,providers:[{provide:Xc,useValue:i},{provide:bA,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var BH=[[["caption"]],[["colgroup"],["col"]],"*"],UH=["caption","colgroup, col","*"];function HH(t,n){t&1&&H(0,2)}function zH(t,n){t&1&&(v(0,"thead",0),Nt(1,1),_(),v(2,"tbody",0),Nt(3,2)(4,3),_(),v(5,"tfoot",0),Nt(6,4),_())}function $H(t,n){t&1&&Nt(0,1)(1,2)(2,3)(3,4)}var qn=new g("CDK_TABLE");var pm=(()=>{class t{template=d(je);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","cdkCellDef",""]]})}return t})(),gm=(()=>{class t{template=d(je);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","cdkHeaderCellDef",""]]})}return t})(),CA=(()=>{class t{template=d(je);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","cdkFooterCellDef",""]]})}return t})(),Aa=(()=>{class t{_table=d(qn,{optional:!0});_hasStickyChanged=!1;get name(){return this._name}set name(e){this._setNameInput(e)}_name;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;get stickyEnd(){return this._stickyEnd}set stickyEnd(e){e!==this._stickyEnd&&(this._stickyEnd=e,this._hasStickyChanged=!0)}_stickyEnd=!1;cell;headerCell;footerCell;cssClassFriendlyName;_columnCssClassName;constructor(){}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}_updateColumnCssClassName(){this._columnCssClassName=[`cdk-column-${this.cssClassFriendlyName}`]}_setNameInput(e){e&&(this._name=e,this.cssClassFriendlyName=e.replace(/[^a-z0-9_-]/gi,"-"),this._updateColumnCssClassName())}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","cdkColumnDef",""]],contentQueries:function(i,r,o){if(i&1&&mt(o,pm,5)(o,gm,5)(o,CA,5),i&2){let s;q(s=Y())&&(r.cell=s.first),q(s=Y())&&(r.headerCell=s.first),q(s=Y())&&(r.footerCell=s.first)}},inputs:{name:[0,"cdkColumnDef","name"],sticky:[2,"sticky","sticky",L],stickyEnd:[2,"stickyEnd","stickyEnd",L]}})}return t})(),mm=class{constructor(n,e){e.nativeElement.classList.add(...n._columnCssClassName)}},EA=(()=>{class t extends mm{constructor(){super(d(Aa),d(O))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["cdk-header-cell"],["th","cdk-header-cell",""]],hostAttrs:["role","columnheader",1,"cdk-header-cell"],features:[oe]})}return t})();var xA=(()=>{class t extends mm{constructor(){let e=d(Aa),i=d(O);super(e,i);let r=e._table?._getCellRole();r&&i.nativeElement.setAttribute("role",r)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["cdk-cell"],["td","cdk-cell",""]],hostAttrs:[1,"cdk-cell"],features:[oe]})}return t})();var zb=(()=>{class t{template=d(je);_differs=d(Fi);columns;_columnsDiffer;constructor(){}ngOnChanges(e){if(!this._columnsDiffer){let i=e.columns&&e.columns.currentValue||[];this._columnsDiffer=this._differs.find(i).create(),this._columnsDiffer.diff(i)}}getColumnsDiff(){return this._columnsDiffer.diff(this.columns)}extractCellTemplate(e){return this instanceof ed?e.headerCell.template:this instanceof $b?e.footerCell.template:e.cell.template}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,features:[Ie]})}return t})(),ed=(()=>{class t extends zb{_table=d(qn,{optional:!0});_hasStickyChanged=!1;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;constructor(){super(d(je),d(Fi))}ngOnChanges(e){super.ngOnChanges(e)}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","cdkHeaderRowDef",""]],inputs:{columns:[0,"cdkHeaderRowDef","columns"],sticky:[2,"cdkHeaderRowDefSticky","sticky",L]},features:[oe,Ie]})}return t})(),$b=(()=>{class t extends zb{_table=d(qn,{optional:!0});_hasStickyChanged=!1;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;constructor(){super(d(je),d(Fi))}ngOnChanges(e){super.ngOnChanges(e)}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","cdkFooterRowDef",""]],inputs:{columns:[0,"cdkFooterRowDef","columns"],sticky:[2,"cdkFooterRowDefSticky","sticky",L]},features:[oe,Ie]})}return t})(),vm=(()=>{class t extends zb{_table=d(qn,{optional:!0});when;constructor(){super(d(je),d(Fi))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","cdkRowDef",""]],inputs:{columns:[0,"cdkRowDefColumns","columns"],when:[0,"cdkRowDefWhen","when"]},features:[oe]})}return t})(),Ko=(()=>{class t{_viewContainer=d(Qe);cells;context;static mostRecentCellOutlet=null;constructor(){t.mostRecentCellOutlet=this}ngOnDestroy(){t.mostRecentCellOutlet===this&&(t.mostRecentCellOutlet=null)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","cdkCellOutlet",""]]})}return t})(),Gb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["cdk-header-row"],["tr","cdk-header-row",""]],hostAttrs:["role","row",1,"cdk-header-row"],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&Nt(0,0)},dependencies:[Ko],encapsulation:2})}return t})();var Wb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["cdk-row"],["tr","cdk-row",""]],hostAttrs:["role","row",1,"cdk-row"],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&Nt(0,0)},dependencies:[Ko],encapsulation:2})}return t})(),SA=(()=>{class t{templateRef=d(je);_contentClassNames=["cdk-no-data-row","cdk-row"];_cellClassNames=["cdk-cell","cdk-no-data-cell"];_cellSelector="td, cdk-cell, [cdk-cell], .cdk-cell";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["ng-template","cdkNoDataRow",""]]})}return t})(),DA=["top","bottom","left","right"],Hb=class{_isNativeHtmlTable;_stickCellCss;_isBrowser;_needsPositionStickyOnElement;direction;_positionListener;_tableInjector;_elemSizeCache=new WeakMap;_resizeObserver=globalThis?.ResizeObserver?new globalThis.ResizeObserver(n=>this._updateCachedSizes(n)):null;_updatedStickyColumnsParamsToReplay=[];_stickyColumnsReplayTimeout=null;_cachedCellWidths=[];_borderCellCss;_destroyed=!1;constructor(n,e,i=!0,r=!0,o,s,a){this._isNativeHtmlTable=n,this._stickCellCss=e,this._isBrowser=i,this._needsPositionStickyOnElement=r,this.direction=o,this._positionListener=s,this._tableInjector=a,this._borderCellCss={top:`${e}-border-elem-top`,bottom:`${e}-border-elem-bottom`,left:`${e}-border-elem-left`,right:`${e}-border-elem-right`}}clearStickyPositioning(n,e){(e.includes("left")||e.includes("right"))&&this._removeFromStickyColumnReplayQueue(n);let i=[];for(let r of n)r.nodeType===r.ELEMENT_NODE&&i.push(r,...Array.from(r.children));Ke({write:()=>{for(let r of i)this._removeStickyStyle(r,e)}},{injector:this._tableInjector})}updateStickyColumns(n,e,i,r=!0,o=!0){if(!n.length||!this._isBrowser||!(e.some(E=>E)||i.some(E=>E))){this._positionListener?.stickyColumnsUpdated({sizes:[]}),this._positionListener?.stickyEndColumnsUpdated({sizes:[]});return}let s=n[0],a=s.children.length,l=this.direction==="rtl",c=l?"right":"left",u=l?"left":"right",f=e.lastIndexOf(!0),h=i.indexOf(!0),m,p,w;o&&this._updateStickyColumnReplayQueue({rows:[...n],stickyStartStates:[...e],stickyEndStates:[...i]}),Ke({earlyRead:()=>{m=this._getCellWidths(s,r),p=this._getStickyStartColumnPositions(m,e),w=this._getStickyEndColumnPositions(m,i)},write:()=>{for(let E of n)for(let I=0;I<a;I++){let ce=E.children[I];e[I]&&this._addStickyStyle(ce,c,p[I],I===f),i[I]&&this._addStickyStyle(ce,u,w[I],I===h)}this._positionListener&&m.some(E=>!!E)&&(this._positionListener.stickyColumnsUpdated({sizes:f===-1?[]:m.slice(0,f+1).map((E,I)=>e[I]?E:null)}),this._positionListener.stickyEndColumnsUpdated({sizes:h===-1?[]:m.slice(h).map((E,I)=>i[I+h]?E:null).reverse()}))}},{injector:this._tableInjector})}stickRows(n,e,i){if(!this._isBrowser)return;let r=i==="bottom"?n.slice().reverse():n,o=i==="bottom"?e.slice().reverse():e,s=[],a=[],l=[];Ke({earlyRead:()=>{for(let c=0,u=0;c<r.length;c++){if(!o[c])continue;s[c]=u;let f=r[c];l[c]=this._isNativeHtmlTable?Array.from(f.children):[f];let h=this._retrieveElementSize(f).height;u+=h,a[c]=h}},write:()=>{let c=o.lastIndexOf(!0);for(let u=0;u<r.length;u++){if(!o[u])continue;let f=s[u],h=u===c;for(let m of l[u])this._addStickyStyle(m,i,f,h)}i==="top"?this._positionListener?.stickyHeaderRowsUpdated({sizes:a,offsets:s,elements:l}):this._positionListener?.stickyFooterRowsUpdated({sizes:a,offsets:s,elements:l})}},{injector:this._tableInjector})}updateStickyFooterContainer(n,e){this._isNativeHtmlTable&&Ke({write:()=>{let i=n.querySelector("tfoot");i&&(e.some(r=>!r)?this._removeStickyStyle(i,["bottom"]):this._addStickyStyle(i,"bottom",0,!1))}},{injector:this._tableInjector})}destroy(){this._stickyColumnsReplayTimeout&&clearTimeout(this._stickyColumnsReplayTimeout),this._resizeObserver?.disconnect(),this._destroyed=!0}_removeStickyStyle(n,e){if(!n.classList.contains(this._stickCellCss))return;for(let r of e)n.style[r]="",n.classList.remove(this._borderCellCss[r]);DA.some(r=>e.indexOf(r)===-1&&n.style[r])?n.style.zIndex=this._getCalculatedZIndex(n):(n.style.zIndex="",this._needsPositionStickyOnElement&&(n.style.position=""),n.classList.remove(this._stickCellCss))}_addStickyStyle(n,e,i,r){n.classList.add(this._stickCellCss),r&&n.classList.add(this._borderCellCss[e]),n.style[e]=`${i}px`,n.style.zIndex=this._getCalculatedZIndex(n),this._needsPositionStickyOnElement&&(n.style.cssText+="position: -webkit-sticky; position: sticky; ")}_getCalculatedZIndex(n){let e={top:100,bottom:10,left:1,right:1},i=0;for(let r of DA)n.style[r]&&(i+=e[r]);return i?`${i}`:""}_getCellWidths(n,e=!0){if(!e&&this._cachedCellWidths.length)return this._cachedCellWidths;let i=[],r=n.children;for(let o=0;o<r.length;o++){let s=r[o];i.push(this._retrieveElementSize(s).width)}return this._cachedCellWidths=i,i}_getStickyStartColumnPositions(n,e){let i=[],r=0;for(let o=0;o<n.length;o++)e[o]&&(i[o]=r,r+=n[o]);return i}_getStickyEndColumnPositions(n,e){let i=[],r=0;for(let o=n.length;o>0;o--)e[o]&&(i[o]=r,r+=n[o]);return i}_retrieveElementSize(n){let e=this._elemSizeCache.get(n);if(e)return e;let i=n.getBoundingClientRect(),r={width:i.width,height:i.height};return this._resizeObserver&&(this._elemSizeCache.set(n,r),this._resizeObserver.observe(n,{box:"border-box"})),r}_updateStickyColumnReplayQueue(n){this._removeFromStickyColumnReplayQueue(n.rows),this._stickyColumnsReplayTimeout||this._updatedStickyColumnsParamsToReplay.push(n)}_removeFromStickyColumnReplayQueue(n){let e=new Set(n);for(let i of this._updatedStickyColumnsParamsToReplay)i.rows=i.rows.filter(r=>!e.has(r));this._updatedStickyColumnsParamsToReplay=this._updatedStickyColumnsParamsToReplay.filter(i=>!!i.rows.length)}_updateCachedSizes(n){let e=!1;for(let i of n){let r=i.borderBoxSize?.length?{width:i.borderBoxSize[0].inlineSize,height:i.borderBoxSize[0].blockSize}:{width:i.contentRect.width,height:i.contentRect.height};r.width!==this._elemSizeCache.get(i.target)?.width&&GH(i.target)&&(e=!0),this._elemSizeCache.set(i.target,r)}e&&this._updatedStickyColumnsParamsToReplay.length&&(this._stickyColumnsReplayTimeout&&clearTimeout(this._stickyColumnsReplayTimeout),this._stickyColumnsReplayTimeout=setTimeout(()=>{if(!this._destroyed){for(let i of this._updatedStickyColumnsParamsToReplay)this.updateStickyColumns(i.rows,i.stickyStartStates,i.stickyEndStates,!0,!1);this._updatedStickyColumnsParamsToReplay=[],this._stickyColumnsReplayTimeout=null}},0))}};function GH(t){return["cdk-cell","cdk-header-cell","cdk-footer-cell"].some(n=>t.classList.contains(n))}var Jc=new g("STICKY_POSITIONING_LISTENER");var qb=(()=>{class t{viewContainer=d(Qe);elementRef=d(O);constructor(){let e=d(qn);e._rowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","rowOutlet",""]]})}return t})(),Yb=(()=>{class t{viewContainer=d(Qe);elementRef=d(O);constructor(){let e=d(qn);e._headerRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","headerRowOutlet",""]]})}return t})(),Zb=(()=>{class t{viewContainer=d(Qe);elementRef=d(O);constructor(){let e=d(qn);e._footerRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","footerRowOutlet",""]]})}return t})(),Kb=(()=>{class t{viewContainer=d(Qe);elementRef=d(O);constructor(){let e=d(qn);e._noDataRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["","noDataRowOutlet",""]]})}return t})(),Qb=(()=>{class t{_differs=d(Fi);_changeDetectorRef=d(Pe);_elementRef=d(O);_dir=d(bt,{optional:!0});_platform=d(Ce);_viewRepeater;_viewportRuler=d(pi);_injector=d(W);_virtualScrollViewport=d(eM,{optional:!0,host:!0});_positionListener=d(Jc,{optional:!0})||d(Jc,{optional:!0,skipSelf:!0});_document=d(P);_data;_renderedRange;_onDestroy=new x;_renderRows;_renderChangeSubscription=null;_columnDefsByName=new Map;_rowDefs;_headerRowDefs;_footerRowDefs;_dataDiffer;_defaultRowDef=null;_customColumnDefs=new Set;_customRowDefs=new Set;_customHeaderRowDefs=new Set;_customFooterRowDefs=new Set;_customNoDataRow=null;_headerRowDefChanged=!0;_footerRowDefChanged=!0;_stickyColumnStylesNeedReset=!0;_forceRecalculateCellWidths=!0;_cachedRenderRowsMap=new Map;_isNativeHtmlTable;_stickyStyler;stickyCssClass="cdk-table-sticky";needsPositionStickyOnElement=!0;_isServer;_isShowingNoDataRow=!1;_hasAllOutlets=!1;_hasInitialized=!1;_headerRowStickyUpdates=new x;_footerRowStickyUpdates=new x;_disableVirtualScrolling=!1;_getCellRole(){if(this._cellRoleInternal===void 0){let e=this._elementRef.nativeElement.getAttribute("role");return e==="grid"||e==="treegrid"?"gridcell":"cell"}return this._cellRoleInternal}_cellRoleInternal=void 0;get trackBy(){return this._trackByFn}set trackBy(e){this._trackByFn=e}_trackByFn;get dataSource(){return this._dataSource}set dataSource(e){this._dataSource!==e&&(this._switchDataSource(e),this._changeDetectorRef.markForCheck())}_dataSource;_dataSourceChanges=new x;_dataStream=new x;get multiTemplateDataRows(){return this._multiTemplateDataRows}set multiTemplateDataRows(e){this._multiTemplateDataRows=e,this._rowOutlet&&this._rowOutlet.viewContainer.length&&(this._forceRenderDataRows(),this.updateStickyColumnStyles())}_multiTemplateDataRows=!1;get fixedLayout(){return this._virtualScrollEnabled()?!0:this._fixedLayout}set fixedLayout(e){this._fixedLayout=e,this._forceRecalculateCellWidths=!0,this._stickyColumnStylesNeedReset=!0}_fixedLayout=!1;recycleRows=!1;contentChanged=new j;viewChange=new rt({start:0,end:Number.MAX_VALUE});_rowOutlet;_headerRowOutlet;_footerRowOutlet;_noDataRowOutlet;_contentColumnDefs;_contentRowDefs;_contentHeaderRowDefs;_contentFooterRowDefs;_noDataRow;constructor(){d(new Zt("role"),{optional:!0})||this._elementRef.nativeElement.setAttribute("role","table"),this._isServer=!this._platform.isBrowser,this._isNativeHtmlTable=this._elementRef.nativeElement.nodeName==="TABLE",this._dataDiffer=this._differs.find([]).create((i,r)=>this.trackBy?this.trackBy(r.dataIndex,r.data):r)}ngOnInit(){this._setupStickyStyler(),this._viewportRuler.change().pipe(Ae(this._onDestroy)).subscribe(()=>{this._forceRecalculateCellWidths=!0})}ngAfterContentInit(){this._viewRepeater=this.recycleRows||this._virtualScrollEnabled()?new _h:new rm,this._virtualScrollEnabled()&&this._setupVirtualScrolling(this._virtualScrollViewport),this._hasInitialized=!0}ngAfterContentChecked(){this._canRender()&&this._render()}ngOnDestroy(){this._stickyStyler?.destroy(),[this._rowOutlet?.viewContainer,this._headerRowOutlet?.viewContainer,this._footerRowOutlet?.viewContainer,this._cachedRenderRowsMap,this._customColumnDefs,this._customRowDefs,this._customHeaderRowDefs,this._customFooterRowDefs,this._columnDefsByName].forEach(e=>{e?.clear()}),this._headerRowDefs=[],this._footerRowDefs=[],this._defaultRowDef=null,this._headerRowStickyUpdates.complete(),this._footerRowStickyUpdates.complete(),this._onDestroy.next(),this._onDestroy.complete(),vh(this.dataSource)&&this.dataSource.disconnect(this)}renderRows(){this._renderRows=this._getAllRenderRows();let e=this._dataDiffer.diff(this._renderRows);if(!e){this._updateNoDataRow(),this.contentChanged.next();return}let i=this._rowOutlet.viewContainer;this._viewRepeater.applyChanges(e,i,(r,o,s)=>this._getEmbeddedViewArgs(r.item,s),r=>r.item.data,r=>{r.operation===zn.INSERTED&&r.context&&this._renderCellTemplateForItem(r.record.item.rowDef,r.context)}),this._updateRowIndexContext(),e.forEachIdentityChange(r=>{let o=i.get(r.currentIndex);o.context.$implicit=r.item.data}),this._updateNoDataRow(),this.contentChanged.next(),this.updateStickyColumnStyles()}addColumnDef(e){this._customColumnDefs.add(e)}removeColumnDef(e){this._customColumnDefs.delete(e)}addRowDef(e){this._customRowDefs.add(e)}removeRowDef(e){this._customRowDefs.delete(e)}addHeaderRowDef(e){this._customHeaderRowDefs.add(e),this._headerRowDefChanged=!0}removeHeaderRowDef(e){this._customHeaderRowDefs.delete(e),this._headerRowDefChanged=!0}addFooterRowDef(e){this._customFooterRowDefs.add(e),this._footerRowDefChanged=!0}removeFooterRowDef(e){this._customFooterRowDefs.delete(e),this._footerRowDefChanged=!0}setNoDataRow(e){this._customNoDataRow=e}updateStickyHeaderRowStyles(){let e=this._getRenderedRows(this._headerRowOutlet);if(this._isNativeHtmlTable){let r=wA(this._headerRowOutlet,"thead");r&&(r.style.display=e.length?"":"none")}let i=this._headerRowDefs.map(r=>r.sticky);this._stickyStyler.clearStickyPositioning(e,["top"]),this._stickyStyler.stickRows(e,i,"top"),this._headerRowDefs.forEach(r=>r.resetStickyChanged())}updateStickyFooterRowStyles(){let e=this._getRenderedRows(this._footerRowOutlet);if(this._isNativeHtmlTable){let r=wA(this._footerRowOutlet,"tfoot");r&&(r.style.display=e.length?"":"none")}let i=this._footerRowDefs.map(r=>r.sticky);this._stickyStyler.clearStickyPositioning(e,["bottom"]),this._stickyStyler.stickRows(e,i,"bottom"),this._stickyStyler.updateStickyFooterContainer(this._elementRef.nativeElement,i),this._footerRowDefs.forEach(r=>r.resetStickyChanged())}updateStickyColumnStyles(){let e=this._getRenderedRows(this._headerRowOutlet),i=this._getRenderedRows(this._rowOutlet),r=this._getRenderedRows(this._footerRowOutlet);(this._isNativeHtmlTable&&!this.fixedLayout||this._stickyColumnStylesNeedReset)&&(this._stickyStyler.clearStickyPositioning([...e,...i,...r],["left","right"]),this._stickyColumnStylesNeedReset=!1),e.forEach((o,s)=>{this._addStickyColumnStyles([o],this._headerRowDefs[s])}),this._rowDefs.forEach(o=>{let s=[];for(let a=0;a<i.length;a++)this._renderRows[a].rowDef===o&&s.push(i[a]);this._addStickyColumnStyles(s,o)}),r.forEach((o,s)=>{this._addStickyColumnStyles([o],this._footerRowDefs[s])}),Array.from(this._columnDefsByName.values()).forEach(o=>o.resetStickyChanged())}stickyColumnsUpdated(e){this._positionListener?.stickyColumnsUpdated(e)}stickyEndColumnsUpdated(e){this._positionListener?.stickyEndColumnsUpdated(e)}stickyHeaderRowsUpdated(e){this._headerRowStickyUpdates.next(e),this._positionListener?.stickyHeaderRowsUpdated(e)}stickyFooterRowsUpdated(e){this._footerRowStickyUpdates.next(e),this._positionListener?.stickyFooterRowsUpdated(e)}_outletAssigned(){!this._hasAllOutlets&&this._rowOutlet&&this._headerRowOutlet&&this._footerRowOutlet&&this._noDataRowOutlet&&(this._hasAllOutlets=!0,this._canRender()&&this._render())}_canRender(){return this._hasAllOutlets&&this._hasInitialized}_render(){this._cacheRowDefs(),this._cacheColumnDefs(),!this._headerRowDefs.length&&!this._footerRowDefs.length&&this._rowDefs.length;let i=this._renderUpdatedColumns()||this._headerRowDefChanged||this._footerRowDefChanged;this._stickyColumnStylesNeedReset=this._stickyColumnStylesNeedReset||i,this._forceRecalculateCellWidths=i,this._headerRowDefChanged&&(this._forceRenderHeaderRows(),this._headerRowDefChanged=!1),this._footerRowDefChanged&&(this._forceRenderFooterRows(),this._footerRowDefChanged=!1),this.dataSource&&this._rowDefs.length>0&&!this._renderChangeSubscription?this._observeRenderChanges():this._stickyColumnStylesNeedReset&&this.updateStickyColumnStyles(),this._checkStickyStates()}_getAllRenderRows(){if(!Array.isArray(this._data)||!this._renderedRange)return[];let e=[],i=Math.min(this._data.length,this._renderedRange.end),r=this._cachedRenderRowsMap;this._cachedRenderRowsMap=new Map;for(let o=this._renderedRange.start;o<i;o++){let s=this._data[o],a=this._getRenderRowsForData(s,o,r.get(s));this._cachedRenderRowsMap.has(s)||this._cachedRenderRowsMap.set(s,new WeakMap);for(let l=0;l<a.length;l++){let c=a[l],u=this._cachedRenderRowsMap.get(c.data);u.has(c.rowDef)?u.get(c.rowDef).push(c):u.set(c.rowDef,[c]),e.push(c)}}return e}_getRenderRowsForData(e,i,r){return this._getRowDefs(e,i).map(s=>{let a=r&&r.has(s)?r.get(s):[];if(a.length){let l=a.shift();return l.dataIndex=i,l}else return{data:e,rowDef:s,dataIndex:i}})}_cacheColumnDefs(){this._columnDefsByName.clear(),hm(this._getOwnDefs(this._contentColumnDefs),this._customColumnDefs).forEach(i=>{this._columnDefsByName.has(i.name),this._columnDefsByName.set(i.name,i)})}_cacheRowDefs(){this._headerRowDefs=hm(this._getOwnDefs(this._contentHeaderRowDefs),this._customHeaderRowDefs),this._footerRowDefs=hm(this._getOwnDefs(this._contentFooterRowDefs),this._customFooterRowDefs),this._rowDefs=hm(this._getOwnDefs(this._contentRowDefs),this._customRowDefs);let e=this._rowDefs.filter(i=>!i.when);this._defaultRowDef=e[0]}_renderUpdatedColumns(){let e=(s,a)=>{let l=!!a.getColumnsDiff();return s||l},i=this._rowDefs.reduce(e,!1);i&&this._forceRenderDataRows();let r=this._headerRowDefs.reduce(e,!1);r&&this._forceRenderHeaderRows();let o=this._footerRowDefs.reduce(e,!1);return o&&this._forceRenderFooterRows(),i||r||o}_switchDataSource(e){this._data=[],vh(this.dataSource)&&this.dataSource.disconnect(this),this._renderChangeSubscription&&(this._renderChangeSubscription.unsubscribe(),this._renderChangeSubscription=null),e||(this._dataDiffer&&this._dataDiffer.diff([]),this._rowOutlet&&this._rowOutlet.viewContainer.clear()),this._dataSource=e}_observeRenderChanges(){if(!this.dataSource)return;let e;vh(this.dataSource)?e=this.dataSource.connect(this):eo(this.dataSource)?e=this.dataSource:Array.isArray(this.dataSource)&&(e=B(this.dataSource)),this._renderChangeSubscription=Kn([e,this.viewChange]).pipe(Ae(this._onDestroy)).subscribe(([i,r])=>{this._data=i||[],this._renderedRange=r,this._dataStream.next(i),this.renderRows()})}_forceRenderHeaderRows(){this._headerRowOutlet.viewContainer.length>0&&this._headerRowOutlet.viewContainer.clear(),this._headerRowDefs.forEach((e,i)=>this._renderRow(this._headerRowOutlet,e,i)),this.updateStickyHeaderRowStyles()}_forceRenderFooterRows(){this._footerRowOutlet.viewContainer.length>0&&this._footerRowOutlet.viewContainer.clear(),this._footerRowDefs.forEach((e,i)=>this._renderRow(this._footerRowOutlet,e,i)),this.updateStickyFooterRowStyles()}_addStickyColumnStyles(e,i){let r=Array.from(i?.columns||[]).map(a=>{let l=this._columnDefsByName.get(a);return l}),o=r.map(a=>a.sticky),s=r.map(a=>a.stickyEnd);this._stickyStyler.updateStickyColumns(e,o,s,!this.fixedLayout||this._forceRecalculateCellWidths)}_getRenderedRows(e){let i=[];for(let r=0;r<e.viewContainer.length;r++){let o=e.viewContainer.get(r);i.push(o.rootNodes[0])}return i}_getRowDefs(e,i){if(this._rowDefs.length===1)return[this._rowDefs[0]];let r=[];if(this.multiTemplateDataRows)r=this._rowDefs.filter(o=>!o.when||o.when(i,e));else{let o=this._rowDefs.find(s=>s.when&&s.when(i,e))||this._defaultRowDef;o&&r.push(o)}return r.length,r}_getEmbeddedViewArgs(e,i){let r=e.rowDef,o={$implicit:e.data};return{templateRef:r.template,context:o,index:i}}_renderRow(e,i,r,o={}){let s=e.viewContainer.createEmbeddedView(i.template,o,r);return this._renderCellTemplateForItem(i,o),s}_renderCellTemplateForItem(e,i){for(let r of this._getCellTemplates(e))Ko.mostRecentCellOutlet&&Ko.mostRecentCellOutlet._viewContainer.createEmbeddedView(r,i);this._changeDetectorRef.markForCheck()}_updateRowIndexContext(){let e=this._rowOutlet.viewContainer;for(let i=0,r=e.length;i<r;i++){let s=e.get(i).context;s.count=r,s.first=i===0,s.last=i===r-1,s.even=i%2===0,s.odd=!s.even,this.multiTemplateDataRows?(s.dataIndex=this._renderRows[i].dataIndex,s.renderIndex=i):s.index=this._renderRows[i].dataIndex}}_getCellTemplates(e){return!e||!e.columns?[]:Array.from(e.columns,i=>{let r=this._columnDefsByName.get(i);return e.extractCellTemplate(r)})}_forceRenderDataRows(){this._dataDiffer.diff([]),this._rowOutlet.viewContainer.clear(),this.renderRows()}_checkStickyStates(){let e=(i,r)=>i||r.hasStickyChanged();this._headerRowDefs.reduce(e,!1)&&this.updateStickyHeaderRowStyles(),this._footerRowDefs.reduce(e,!1)&&this.updateStickyFooterRowStyles(),Array.from(this._columnDefsByName.values()).reduce(e,!1)&&(this._stickyColumnStylesNeedReset=!0,this.updateStickyColumnStyles())}_setupStickyStyler(){let e=this._dir?this._dir.value:"ltr",i=this._injector;this._stickyStyler=new Hb(this._isNativeHtmlTable,this.stickyCssClass,this._platform.isBrowser,this.needsPositionStickyOnElement,e,this,i),(this._dir?this._dir.change:B()).pipe(Ae(this._onDestroy)).subscribe(r=>{this._stickyStyler.direction=r,this.updateStickyColumnStyles()})}_setupVirtualScrolling(e){let i=typeof requestAnimationFrame<"u"?gd:hd;this.viewChange.next({start:0,end:0}),e.renderedRangeStream.pipe(as(0,i),Ae(this._onDestroy)).subscribe(this.viewChange),e.attach({dataStream:this._dataStream,measureRangeSize:(r,o)=>this._measureRangeSize(r,o)}),Kn([e.renderedContentOffset,this._headerRowStickyUpdates]).pipe(Ae(this._onDestroy)).subscribe(([r,o])=>{if(!(!o.sizes||!o.offsets||!o.elements))for(let s=0;s<o.elements.length;s++){let a=o.elements[s];if(a){let l=o.offsets[s],c=r!==0?Math.max(r-l,l):-l;for(let u of a)u.style.top=`${-c}px`}}}),Kn([e.renderedContentOffset,this._footerRowStickyUpdates]).pipe(Ae(this._onDestroy)).subscribe(([r,o])=>{if(!(!o.sizes||!o.offsets||!o.elements))for(let s=0;s<o.elements.length;s++){let a=o.elements[s];if(a)for(let l of a)l.style.bottom=`${r+o.offsets[s]}px`}})}_getOwnDefs(e){return e.filter(i=>!i._table||i._table===this)}_updateNoDataRow(){let e=this._customNoDataRow||this._noDataRow;if(!e)return;let i=this._rowOutlet.viewContainer.length===0;if(i===this._isShowingNoDataRow)return;let r=this._noDataRowOutlet.viewContainer;if(i){let o=r.createEmbeddedView(e.templateRef),s=o.rootNodes[0];if(o.rootNodes.length===1&&s?.nodeType===this._document.ELEMENT_NODE){s.setAttribute("role","row"),s.classList.add(...e._contentClassNames);let a=s.querySelectorAll(e._cellSelector);for(let l=0;l<a.length;l++)a[l].classList.add(...e._cellClassNames)}}else r.clear();this._isShowingNoDataRow=i,this._changeDetectorRef.markForCheck()}_measureRangeSize(e,i){if(e.start>=e.end||i!=="vertical")return 0;let r=this.viewChange.value,o=this._rowOutlet.viewContainer;e.start<r.start||e.end>r.end;let s=e.start-r.start,a=e.end-e.start,l,c;for(let h=0;h<a;h++){let m=o.get(h+s);if(m&&m.rootNodes.length){l=c=m.rootNodes[0];break}}for(let h=a-1;h>-1;h--){let m=o.get(h+s);if(m&&m.rootNodes.length){c=m.rootNodes[m.rootNodes.length-1];break}}let u=l?.getBoundingClientRect?.(),f=c?.getBoundingClientRect?.();return u&&f?f.bottom-u.top:0}_virtualScrollEnabled(){return!this._disableVirtualScrolling&&this._virtualScrollViewport!=null}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["cdk-table"],["table","cdk-table",""]],contentQueries:function(i,r,o){if(i&1&&mt(o,SA,5)(o,Aa,5)(o,vm,5)(o,ed,5)(o,$b,5),i&2){let s;q(s=Y())&&(r._noDataRow=s.first),q(s=Y())&&(r._contentColumnDefs=s),q(s=Y())&&(r._contentRowDefs=s),q(s=Y())&&(r._contentHeaderRowDefs=s),q(s=Y())&&(r._contentFooterRowDefs=s)}},hostAttrs:[1,"cdk-table"],hostVars:2,hostBindings:function(i,r){i&2&&Z("cdk-table-fixed-layout",r.fixedLayout)},inputs:{trackBy:"trackBy",dataSource:"dataSource",multiTemplateDataRows:[2,"multiTemplateDataRows","multiTemplateDataRows",L],fixedLayout:[2,"fixedLayout","fixedLayout",L],recycleRows:[2,"recycleRows","recycleRows",L]},outputs:{contentChanged:"contentChanged"},exportAs:["cdkTable"],features:[_e([{provide:qn,useExisting:t},{provide:Jc,useValue:null}])],ngContentSelectors:UH,decls:5,vars:2,consts:[["role","rowgroup"],["headerRowOutlet",""],["rowOutlet",""],["noDataRowOutlet",""],["footerRowOutlet",""]],template:function(i,r){i&1&&(De(BH),H(0),H(1,1),z(2,HH,1,0),z(3,zH,7,0)(4,$H,4,0)),i&2&&(D(2),$(r._isServer?2:-1),D(),$(r._isNativeHtmlTable?3:4))},dependencies:[Yb,qb,Kb,Zb],styles:[`.cdk-table-fixed-layout {
  table-layout: fixed;
}
`],encapsulation:2})}return t})();function hm(t,n){return t.concat(Array.from(n))}function wA(t,n){let e=n.toUpperCase(),i=t.viewContainer.element.nativeElement;for(;i;){let r=i.nodeType===1?i.nodeName:null;if(r===e)return i;if(r==="TABLE")break;i=i.parentNode}return null}var IA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[mc]})}return t})();var WH=[[["caption"]],[["colgroup"],["col"]],"*"],qH=["caption","colgroup, col","*"];function YH(t,n){t&1&&H(0,2)}function ZH(t,n){t&1&&(v(0,"thead",0),Nt(1,1),_(),v(2,"tbody",2),Nt(3,3)(4,4),_(),v(5,"tfoot",0),Nt(6,5),_())}function KH(t,n){t&1&&Nt(0,1)(1,3)(2,4)(3,5)}var _m=(()=>{class t extends Qb{stickyCssClass="mat-mdc-table-sticky";needsPositionStickyOnElement=!1;static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-table"],["table","mat-table",""]],hostAttrs:[1,"mat-mdc-table","mdc-data-table__table"],hostVars:2,hostBindings:function(i,r){i&2&&Z("mat-table-fixed-layout",r.fixedLayout)},exportAs:["matTable"],features:[_e([{provide:Qb,useExisting:t},{provide:qn,useExisting:t},{provide:Jc,useValue:null}]),oe],ngContentSelectors:qH,decls:5,vars:2,consts:[["role","rowgroup"],["headerRowOutlet",""],["role","rowgroup",1,"mdc-data-table__content"],["rowOutlet",""],["noDataRowOutlet",""],["footerRowOutlet",""]],template:function(i,r){i&1&&(De(WH),H(0),H(1,1),z(2,YH,1,0),z(3,ZH,7,0)(4,KH,4,0)),i&2&&(D(2),$(r._isServer?2:-1),D(),$(r._isNativeHtmlTable?3:4))},dependencies:[Yb,qb,Kb,Zb],styles:[`.mat-mdc-table-sticky {
  position: sticky !important;
}

mat-table {
  display: block;
}

mat-header-row {
  min-height: var(--mat-table-header-container-height, 56px);
}

mat-row {
  min-height: var(--mat-table-row-item-container-height, 52px);
}

mat-footer-row {
  min-height: var(--mat-table-footer-container-height, 52px);
}

mat-row, mat-header-row, mat-footer-row {
  display: flex;
  border-width: 0;
  border-bottom-width: 1px;
  border-style: solid;
  align-items: center;
  box-sizing: border-box;
}

mat-cell:first-of-type, mat-header-cell:first-of-type, mat-footer-cell:first-of-type {
  padding-left: 24px;
}
[dir=rtl] mat-cell:first-of-type:not(:only-of-type), [dir=rtl] mat-header-cell:first-of-type:not(:only-of-type), [dir=rtl] mat-footer-cell:first-of-type:not(:only-of-type) {
  padding-left: 0;
  padding-right: 24px;
}
mat-cell:last-of-type, mat-header-cell:last-of-type, mat-footer-cell:last-of-type {
  padding-right: 24px;
}
[dir=rtl] mat-cell:last-of-type:not(:only-of-type), [dir=rtl] mat-header-cell:last-of-type:not(:only-of-type), [dir=rtl] mat-footer-cell:last-of-type:not(:only-of-type) {
  padding-right: 0;
  padding-left: 24px;
}

mat-cell, mat-header-cell, mat-footer-cell {
  flex: 1;
  display: flex;
  align-items: center;
  overflow: hidden;
  word-wrap: break-word;
  min-height: inherit;
}

.mat-mdc-table {
  min-width: 100%;
  border: 0;
  border-spacing: 0;
  table-layout: auto;
  white-space: normal;
  background-color: var(--mat-table-background-color, var(--mat-sys-surface));
}

.mat-table-fixed-layout {
  table-layout: fixed;
}

.mdc-data-table__cell {
  box-sizing: border-box;
  overflow: hidden;
  text-align: start;
  text-overflow: ellipsis;
}

.mdc-data-table__cell,
.mdc-data-table__header-cell {
  padding: 0 16px;
}

.mat-mdc-header-row {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  height: var(--mat-table-header-container-height, 56px);
  color: var(--mat-table-header-headline-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-table-header-headline-font, var(--mat-sys-title-small-font, Roboto, sans-serif));
  line-height: var(--mat-table-header-headline-line-height, var(--mat-sys-title-small-line-height));
  font-size: var(--mat-table-header-headline-size, var(--mat-sys-title-small-size, 14px));
  font-weight: var(--mat-table-header-headline-weight, var(--mat-sys-title-small-weight, 500));
}

.mat-mdc-row {
  height: var(--mat-table-row-item-container-height, 52px);
  color: var(--mat-table-row-item-label-text-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
}

.mat-mdc-row,
.mdc-data-table__content {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-table-row-item-label-text-font, var(--mat-sys-body-medium-font, Roboto, sans-serif));
  line-height: var(--mat-table-row-item-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-table-row-item-label-text-size, var(--mat-sys-body-medium-size, 14px));
  font-weight: var(--mat-table-row-item-label-text-weight, var(--mat-sys-body-medium-weight));
}

.mat-mdc-footer-row {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  height: var(--mat-table-footer-container-height, 52px);
  color: var(--mat-table-row-item-label-text-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-table-footer-supporting-text-font, var(--mat-sys-body-medium-font, Roboto, sans-serif));
  line-height: var(--mat-table-footer-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-table-footer-supporting-text-size, var(--mat-sys-body-medium-size, 14px));
  font-weight: var(--mat-table-footer-supporting-text-weight, var(--mat-sys-body-medium-weight));
  letter-spacing: var(--mat-table-footer-supporting-text-tracking, var(--mat-sys-body-medium-tracking));
}

.mat-mdc-header-cell {
  border-bottom-color: var(--mat-table-row-item-outline-color, var(--mat-sys-outline, rgba(0, 0, 0, 0.12)));
  border-bottom-width: var(--mat-table-row-item-outline-width, 1px);
  border-bottom-style: solid;
  letter-spacing: var(--mat-table-header-headline-tracking, var(--mat-sys-title-small-tracking));
  font-weight: inherit;
  line-height: inherit;
  box-sizing: border-box;
  text-overflow: ellipsis;
  overflow: hidden;
  outline: none;
  text-align: start;
}
.mdc-data-table__row:last-child > .mat-mdc-header-cell {
  border-bottom: none;
}

.mat-mdc-cell {
  border-bottom-color: var(--mat-table-row-item-outline-color, var(--mat-sys-outline, rgba(0, 0, 0, 0.12)));
  border-bottom-width: var(--mat-table-row-item-outline-width, 1px);
  border-bottom-style: solid;
  letter-spacing: var(--mat-table-row-item-label-text-tracking, var(--mat-sys-body-medium-tracking));
  line-height: inherit;
}
.mdc-data-table__row:last-child > .mat-mdc-cell {
  border-bottom: none;
}

.mat-mdc-footer-cell {
  letter-spacing: var(--mat-table-row-item-label-text-tracking, var(--mat-sys-body-medium-tracking));
}

mat-row.mat-mdc-row,
mat-header-row.mat-mdc-header-row,
mat-footer-row.mat-mdc-footer-row {
  border-bottom: none;
}

.mat-mdc-table tbody,
.mat-mdc-table tfoot,
.mat-mdc-table thead,
.mat-mdc-cell,
.mat-mdc-footer-cell,
.mat-mdc-header-row,
.mat-mdc-row,
.mat-mdc-footer-row,
.mat-mdc-table .mat-mdc-header-cell {
  background: inherit;
}

.mat-mdc-table mat-header-row.mat-mdc-header-row,
.mat-mdc-table mat-row.mat-mdc-row,
.mat-mdc-table mat-footer-row.mat-mdc-footer-cell {
  height: unset;
}

mat-header-cell.mat-mdc-header-cell,
mat-cell.mat-mdc-cell,
mat-footer-cell.mat-mdc-footer-cell {
  align-self: stretch;
}
`],encapsulation:2})}return t})(),ym=(()=>{class t extends pm{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275dir=C({type:t,selectors:[["","matCellDef",""]],features:[_e([{provide:pm,useExisting:t}]),oe]})}return t})(),bm=(()=>{class t extends gm{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275dir=C({type:t,selectors:[["","matHeaderCellDef",""]],features:[_e([{provide:gm,useExisting:t}]),oe]})}return t})();var Dm=(()=>{class t extends Aa{get name(){return this._name}set name(e){this._setNameInput(e)}_updateColumnCssClassName(){super._updateColumnCssClassName(),this._columnCssClassName.push(`mat-column-${this.cssClassFriendlyName}`)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275dir=C({type:t,selectors:[["","matColumnDef",""]],inputs:{name:[0,"matColumnDef","name"]},features:[_e([{provide:Aa,useExisting:t}]),oe]})}return t})(),wm=(()=>{class t extends EA{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275dir=C({type:t,selectors:[["mat-header-cell"],["th","mat-header-cell",""]],hostAttrs:["role","columnheader",1,"mat-mdc-header-cell","mdc-data-table__header-cell"],features:[oe]})}return t})();var Cm=(()=>{class t extends xA{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275dir=C({type:t,selectors:[["mat-cell"],["td","mat-cell",""]],hostAttrs:[1,"mat-mdc-cell","mdc-data-table__cell"],features:[oe]})}return t})();var Em=(()=>{class t extends ed{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275dir=C({type:t,selectors:[["","matHeaderRowDef",""]],inputs:{columns:[0,"matHeaderRowDef","columns"],sticky:[2,"matHeaderRowDefSticky","sticky",L]},features:[_e([{provide:ed,useExisting:t}]),oe]})}return t})();var xm=(()=>{class t extends vm{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275dir=C({type:t,selectors:[["","matRowDef",""]],inputs:{columns:[0,"matRowDefColumns","columns"],when:[0,"matRowDefWhen","when"]},features:[_e([{provide:vm,useExisting:t}]),oe]})}return t})(),Sm=(()=>{class t extends Gb{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-header-row"],["tr","mat-header-row",""]],hostAttrs:["role","row",1,"mat-mdc-header-row","mdc-data-table__header-row"],exportAs:["matHeaderRow"],features:[_e([{provide:Gb,useExisting:t}]),oe],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&Nt(0,0)},dependencies:[Ko],encapsulation:2})}return t})();var Im=(()=>{class t extends Wb{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Le(t)))(r||t)}})();static \u0275cmp=T({type:t,selectors:[["mat-row"],["tr","mat-row",""]],hostAttrs:["role","row",1,"mat-mdc-row","mdc-data-table__row"],exportAs:["matRow"],features:[_e([{provide:Wb,useExisting:t}]),oe],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&Nt(0,0)},dependencies:[Ko],encapsulation:2})}return t})();var MA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[IA,ye]})}return t})();var QH=["*",[["mat-toolbar-row"]]],XH=["*","mat-toolbar-row"],JH=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=C({type:t,selectors:[["mat-toolbar-row"]],hostAttrs:[1,"mat-toolbar-row"],exportAs:["matToolbarRow"]})}return t})(),AA=(()=>{class t{_elementRef=d(O);_platform=d(Ce);_document=d(P);color;_toolbarRows;constructor(){}ngAfterViewInit(){this._platform.isBrowser&&(this._checkToolbarMixedModes(),this._toolbarRows.changes.subscribe(()=>this._checkToolbarMixedModes()))}_checkToolbarMixedModes(){this._toolbarRows.length}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=T({type:t,selectors:[["mat-toolbar"]],contentQueries:function(i,r,o){if(i&1&&mt(o,JH,5),i&2){let s;q(s=Y())&&(r._toolbarRows=s)}},hostAttrs:[1,"mat-toolbar"],hostVars:6,hostBindings:function(i,r){i&2&&(sn(r.color?"mat-"+r.color:""),Z("mat-toolbar-multiple-rows",r._toolbarRows.length>0)("mat-toolbar-single-row",r._toolbarRows.length===0))},inputs:{color:"color"},exportAs:["matToolbar"],ngContentSelectors:XH,decls:2,vars:0,template:function(i,r){i&1&&(De(QH),H(0),H(1,1))},styles:[`.mat-toolbar {
  background: var(--mat-toolbar-container-background-color, var(--mat-sys-surface));
  color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
}
.mat-toolbar, .mat-toolbar h1, .mat-toolbar h2, .mat-toolbar h3, .mat-toolbar h4, .mat-toolbar h5, .mat-toolbar h6 {
  font-family: var(--mat-toolbar-title-text-font, var(--mat-sys-title-large-font));
  font-size: var(--mat-toolbar-title-text-size, var(--mat-sys-title-large-size));
  line-height: var(--mat-toolbar-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-weight: var(--mat-toolbar-title-text-weight, var(--mat-sys-title-large-weight));
  letter-spacing: var(--mat-toolbar-title-text-tracking, var(--mat-sys-title-large-tracking));
  margin: 0;
}
@media (forced-colors: active) {
  .mat-toolbar {
    outline: solid 1px;
  }
}
.mat-toolbar .mat-form-field-underline,
.mat-toolbar .mat-form-field-ripple,
.mat-toolbar .mat-focused .mat-form-field-ripple {
  background-color: currentColor;
}
.mat-toolbar .mat-form-field-label,
.mat-toolbar .mat-focused .mat-form-field-label,
.mat-toolbar .mat-select-value,
.mat-toolbar .mat-select-arrow,
.mat-toolbar .mat-form-field.mat-focused .mat-select-arrow {
  color: inherit;
}
.mat-toolbar .mat-input-element {
  caret-color: currentColor;
}
.mat-toolbar .mat-mdc-button-base.mat-mdc-button-base.mat-unthemed {
  --mat-button-text-label-text-color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
  --mat-button-outlined-label-text-color: var(--mat-toolbar-container-text-color, var(--mat-sys-on-surface));
}

.mat-toolbar-row, .mat-toolbar-single-row {
  display: flex;
  box-sizing: border-box;
  padding: 0 16px;
  width: 100%;
  flex-direction: row;
  align-items: center;
  white-space: nowrap;
  height: var(--mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-row, .mat-toolbar-single-row {
    height: var(--mat-toolbar-mobile-height, 56px);
  }
}

.mat-toolbar-multiple-rows {
  display: flex;
  box-sizing: border-box;
  flex-direction: column;
  width: 100%;
  min-height: var(--mat-toolbar-standard-height, 64px);
}
@media (max-width: 599px) {
  .mat-toolbar-multiple-rows {
    min-height: var(--mat-toolbar-mobile-height, 56px);
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var RA=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[ye]})}return t})();var Ra=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[zo,Kc,$M,yb,Br,Ab,Vb,Ob,mb,js]})},ka=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[Eb,zo,Kc,MA,js]})},Mm=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[Eb,zo,Kc,yb,Br,Ab,Vb,Ob,mb,js]})},Tm=class t{static \u0275fac=function(e){return new(e||t)};static \u0275mod=R({type:t});static \u0275inj=A({imports:[RA,Kc,vA,yA,NT]})};var tz=new g("WindowLocalStorage",{providedIn:"root",factory:()=>{let t=d(Ai);return Nf(t)?window.localStorage:{length:0,clear:()=>{},getItem:()=>null,key:()=>null,removeItem:()=>{},setItem:()=>{}}}}),Kt=class t{storage=d(tz);setItem(n,e){let i=typeof e=="string"?e:JSON.stringify(e);this.storage.setItem(n,i)}getItem(n){let e=this.storage.getItem(n);if(!e)return null;try{return JSON.parse(e)}catch{return e}}removeItem(n){this.storage.removeItem(n)}clear(){this.storage.clear()}static \u0275fac=function(e){return new(e||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})};var Oa=class t{documento=d(P);localStorageService=d(Kt);theme=k("light");constructor(){kn(()=>{let n=this.theme;this.localStorageService.setItem("app-theme",n()?n():"light");let e=this.documento.documentElement;n()==="dark"?(e.classList.add("dark"),e.classList.remove("light")):(e.classList.add("light"),e.classList.remove("dark"))})}toggleTheme(){this.theme.update(n=>n==="light"?"dark":"light")}setTheme(n){this.theme.set(n)}getTheme(){return this.localStorageService.getItem("app-theme")}static \u0275fac=function(e){return new(e||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})};var tt=class t{static snackBarConfig(){let n=new Ur;return n.duration=12e3,n.panelClass=["btn","btn-outline-dark"],n.verticalPosition="top",n.horizontalPosition="center",n}static openSnackBar(n,e,i){let r=t.snackBarConfig();i.open(n,e,r)}static getMatDialogConf(){let n=new da;return n.disableClose=!1,n.autoFocus=!0,n.exitAnimationDuration="1000ms",n.enterAnimationDuration="1000ms",n}static async generateSHA256(n){var e=new TextEncoder;let i=e.encode(n),r=await window.crypto.subtle.digest("SHA-256",i);return Array.from(new Uint8Array(r)).map(s=>s.toString(16).padStart(2,"0")).join("")}static isSha256(n){return/^[0-9a-fA-F]{64}$/.test(n)}};var F=class{static PRODUCTOS_ID="productoList";static USUARIOS_ID="usuarioList";static LOTES_INVENTARIO_ID="loteList";static PEDIDOS_ID="pedidoList";static DETALLE_PEDIDOS_ID="detallePedidoList";static MOVIMIENTOS_INVENTARIO_ID="movimientoInventarioList";static ESTADO_PEDIDO_ID="estadoPedidoList";static ESTADO_PRODUCTO_ID="estadoProductoList";static TIPO_MOVIMIENTO_ID="tipoMovimientoList";static PASARELA_ID="pasarelaList";static LOGGED_USUARIO="usuario";static LOGGED="logged";static estadoPedidoVacio(){return{id:0,descripcion:""}}static estadoProductoVacio(){return{id:0,descripcion:""}}static tipoMovimientoVacio(){return{id:0,descripcion:""}}static usuarioVacio(){return{id:0,usuario:"",password:"",nombres:"",apellidos:"",fecha_creacion:new Date}}static productoVacio(){return{id:0,sku:"",nombre:"",descripcion:"",precio_venta:0,estado:this.estadoProductoVacio()}}static loteInventarioVacio(){return{id:0,producto:this.productoVacio(),cantidad_inicial:0,cantidad_actual:0,costo_unitario:0,fecha_ingreso:new Date}}static pedidoVacio(){return{id:0,usuario:this.usuarioVacio(),total:0,estado:this.estadoPedidoVacio()}}static detallePedidoVacio(){return{id:0,pedido:this.pedidoVacio(),producto:this.productoVacio(),cantidad:0,precio_unitario_venta:0}}static movimientoInventarioVacio(){return{id:0,producto:this.productoVacio(),lote:this.loteInventarioVacio(),tipo_movimiento:this.tipoMovimientoVacio(),cantidad:0,pedido:this.pedidoVacio(),fecha:new Date}}static pasarelaVacio(){return{id:0,nombre:"",descripcion:"",comision:0}}static getUsuarios(){var n=[],e="";return tt.generateSHA256("1234").then(i=>e=i),n.push({id:1,usuario:"anonim",password:"",nombres:"anonimo",apellidos:"",fecha_creacion:new Date}),n.push({id:2,usuario:"jamileth",password:e,nombres:"Jamileth",apellidos:"Martinez",fecha_creacion:new Date}),n.push({id:3,usuario:"rubix",password:e,nombres:"Rubi",apellidos:"Mejia",fecha_creacion:new Date}),n.push({id:4,usuario:"ale",password:e,nombres:"Alejandra",apellidos:"Guardado",fecha_creacion:new Date}),n.push({id:5,usuario:"khaysernberg",password:e,nombres:"Cesar",apellidos:"Gomez",fecha_creacion:new Date}),n.push({id:6,usuario:"miriam",password:e,nombres:"Mirian",apellidos:"Rivas",fecha_creacion:new Date}),n}static getEstadosPedido(){var n=[];return n.push({id:1,descripcion:"Carrito"}),n.push({id:2,descripcion:"Apartado"}),n.push({id:3,descripcion:"Pagado"}),n}static getEstadosProducto(){var n=[];return n.push({id:1,descripcion:"Disponible"}),n.push({id:2,descripcion:"NoDisponible"}),n.push({id:3,descripcion:"Agotado"}),n}static getTiposMovimiento(){var n=[];return n.push({id:1,descripcion:"Compra"}),n.push({id:2,descripcion:"Venta"}),n.push({id:3,descripcion:"Merma"}),n.push({id:4,descripcion:"DevolucionCompra"}),n.push({id:5,descripcion:"DevolucionVenta"}),n}static getProductos(){var n=[],e=this.getEstadosProducto();return n.push({id:1,sku:"p1",nombre:"producto1",descripcion:"descripcion1",precio_venta:1.5,estado:e[0],stock_local:25,stock_web:125}),n.push({id:2,sku:"p2",nombre:"producto2",descripcion:"descripcion2",precio_venta:2.5,estado:e[0],stock_local:25,stock_web:50}),n.push({id:3,sku:"p3",nombre:"producto3",descripcion:"descripcion3",precio_venta:3.5,estado:e[0],stock_local:25,stock_web:125}),n}static getLotesInventario(){var n=[],e=this.getProductos();return n.push({id:1,producto:e[0],cantidad_inicial:100,cantidad_actual:100,costo_unitario:.9,fecha_ingreso:new Date}),n.push({id:2,producto:e[0],cantidad_inicial:50,cantidad_actual:50,costo_unitario:.95,fecha_ingreso:new Date}),n.push({id:3,producto:e[1],cantidad_inicial:75,cantidad_actual:75,costo_unitario:2,fecha_ingreso:new Date}),n.push({id:4,producto:e[2],cantidad_inicial:150,cantidad_actual:150,costo_unitario:2.75,fecha_ingreso:new Date}),n}static getPedidos(){var n=[],e=this.getUsuarios(),i=this.getEstadosPedido();return n.push({id:1,usuario:e[1],total:10,estado:i[0]}),n.push({id:2,usuario:e[2],total:10,estado:i[0]}),n.push({id:3,usuario:e[3],total:10,estado:i[2]}),n}static getDetallePedidos(){var n=[],e=this.getProductos(),i=this.getPedidos();return n.push({id:1,pedido:i[0],producto:e[0],cantidad:2,precio_unitario_venta:e[0].precio_venta}),n.push({id:2,pedido:i[0],producto:e[2],cantidad:2,precio_unitario_venta:e[2].precio_venta}),n.push({id:3,pedido:i[1],producto:e[0],cantidad:2,precio_unitario_venta:e[0].precio_venta}),n.push({id:4,pedido:i[1],producto:e[2],cantidad:2,precio_unitario_venta:e[2].precio_venta}),n.push({id:5,pedido:i[2],producto:e[0],cantidad:2,precio_unitario_venta:e[0].precio_venta}),n.push({id:6,pedido:i[2],producto:e[2],cantidad:2,precio_unitario_venta:e[2].precio_venta}),n}static getMovimientosInventario(){var n=[],e=this.getProductos(),i=this.getLotesInventario(),r=this.getTiposMovimiento();return n.push({id:1,producto:e[0],lote:i[0],tipo_movimiento:r[0],cantidad:100,pedido:null,fecha:new Date}),n.push({id:2,producto:e[0],lote:i[1],tipo_movimiento:r[0],cantidad:50,pedido:null,fecha:new Date}),n.push({id:3,producto:e[1],lote:i[2],tipo_movimiento:r[0],cantidad:75,pedido:null,fecha:new Date}),n.push({id:4,producto:e[2],lote:i[3],tipo_movimiento:r[0],cantidad:150,pedido:null,fecha:new Date}),n}static getPasarelas(){var n=[];return n.push({id:1,nombre:"Wompi",descripcion:"wompi el salvador",comision:1.5}),n.push({id:2,nombre:"Serfinsa",descripcion:"serfinsa el salvador",comision:1.5}),n.push({id:3,nombre:"PayPal",descripcion:"PayPal international",comision:1.5}),n}};function nz(t,n){t&1&&(v(0,"mat-icon"),M(1,"account_circle"),_())}function iz(t,n){t&1&&(v(0,"mat-icon"),M(1,"menu"),_())}function rz(t,n){if(t&1&&(v(0,"button",3),M(1),_()),t&2){let e=n.$implicit;re("routerLink",e.ruta),D(),et(e.nombre)}}function oz(t,n){if(t&1){let e=Ct();v(0,"button",8),J("click",function(){Ye(e);let r=V();return Ze(r.cerrarSesion())}),v(1,"mat-icon"),M(2,"account_circle_off"),_(),v(3,"span"),M(4,"Cerrar Sesion"),_()(),he(5,"mat-divider")}if(t&2){let e=V();re("hidden",e.logged()==!1)}}var Am=class t{logged=k(!1);isChecked=k(!1);isLightTheme=k(!0);localStorage=d(Kt);themeService=d(Oa);_router=d(Un);menusList=k([]);constructor(){}ngOnInit(){this.cargarMenusDeMantenimientos(),this.validarUsuarioLogeado();var n=this.themeService.getTheme();this.isLightTheme.update(e=>n=="light")}onThemeSwitchChange(){this.themeService.toggleTheme()}routerActivated(n){this.validarUsuarioLogeado()}cargarMenusDeMantenimientos(){var n=[];n.push({id:1,ruta:"/menu/login",nombre:"Iniciar Sesion"}),n.push({id:2,ruta:"/menu/productos",nombre:"Productos"}),n.push({id:3,ruta:"/menu/carrito",nombre:"Carrito"}),this.menusList.update(e=>[...n])}homeClick(){let n={queryParams:{nada:"xd"}};this._router.navigate(["/menu/productos"],n)}cerrarSesion(){let n={queryParams:{logged:"false"}};this.localStorage.setItem(F.LOGGED,"false"),this._router.navigate(["/menu/login"],n)}validarUsuarioLogeado(){let n=this.localStorage.getItem(F.LOGGED);this.logged.update(e=>n==!0)}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-inicio"]],decls:19,vars:14,consts:[["menu","matMenu"],["matIconButton","",3,"matMenuTriggerFor"],["src","logo4.jpg",3,"click"],["mat-menu-item","","routerLinkActive","active-menu-item",3,"routerLink"],[3,"ngModelChange","change","ngModel"],[1,"container-fluid"],[1,"row"],[3,"activate"],["mat-menu-item","",3,"click","hidden"]],template:function(e,i){if(e&1){let r=Ct();v(0,"mat-toolbar")(1,"button",1),z(2,nz,2,0,"mat-icon"),z(3,iz,2,0,"mat-icon"),_(),he(4,"span"),v(5,"span")(6,"img",2),J("click",function(){return i.homeClick()}),_()(),he(7,"span"),_(),v(8,"mat-menu",null,0),Ni(10,rz,2,2,"button",3,Oi),he(12,"mat-divider"),z(13,oz,6,1),v(14,"mat-slide-toggle",4),bf("ngModelChange",function(s){return Ye(r),d_(i.isChecked,s)||(i.isChecked=s),Ze(s)}),J("change",function(){return i.onThemeSwitchChange()}),M(15),_()(),v(16,"div",5)(17,"div",6)(18,"router-outlet",7),J("activate",function(s){return i.routerActivated(s)}),_()()()}if(e&2){let r=ui(9);D(),re("matMenuTriggerFor",r),D(),$(i.logged()?2:-1),D(),$(i.logged()?-1:3),D(),Re("margin-left","1em"),D(2),Re("height","100%")("max-height","2em"),D(),Re("margin-left","1em"),D(3),Pi(i.menusList()),D(3),$(i.logged()?13:-1),D(),yf("ngModel",i.isChecked),D(),$e("Colores: ",i.isChecked()?"Oscuro":"Claro")}},dependencies:[Tm,AA,Sa,Ta,Qc,gA,jb,Pr,hb,ko,Vh,rT,ta,Sy,zo,Nc],encapsulation:2})};var un=class t{localStorageService=d(Kt);constructor(){}getUsuarios(){var n=[],e=this.localStorageService.getItem(F.USUARIOS_ID);return e&&(n=e),(!n||n.length==0)&&(n=F.getUsuarios(),this.localStorageService.setItem(F.USUARIOS_ID,n)),n}getEstadosPedido(){var n=[],e=this.localStorageService.getItem(F.ESTADO_PEDIDO_ID);return e&&(n=e),(!n||n.length==0)&&(n=F.getEstadosPedido(),this.localStorageService.setItem(F.ESTADO_PEDIDO_ID,n)),n}getEstadosProducto(){var n=[],e=this.localStorageService.getItem(F.ESTADO_PRODUCTO_ID);return e&&(n=e),(!n||n.length==0)&&(n=F.getEstadosProducto(),this.localStorageService.setItem(F.ESTADO_PRODUCTO_ID,n)),n}getTiposMovimiento(){var n=[],e=this.localStorageService.getItem(F.TIPO_MOVIMIENTO_ID);return e&&(n=e),(!n||n.length==0)&&(n=F.getTiposMovimiento(),this.localStorageService.setItem(F.TIPO_MOVIMIENTO_ID,n)),n}getProductos(){var n=[],e=this.localStorageService.getItem(F.PRODUCTOS_ID);return e&&(n=e),(!n||n.length==0)&&(n=F.getProductos(),this.localStorageService.setItem(F.PRODUCTOS_ID,n)),n}getLotesInventario(){var n=[],e=this.localStorageService.getItem(F.LOTES_INVENTARIO_ID);return e&&(n=e),(!n||n.length==0)&&(n=F.getLotesInventario(),this.localStorageService.setItem(F.LOTES_INVENTARIO_ID,n)),n}getPedidos(){var n=[],e=this.localStorageService.getItem(F.PEDIDOS_ID);return e&&(n=e),(!n||n.length==0)&&(n=F.getPedidos(),this.localStorageService.setItem(F.PEDIDOS_ID,n)),n}getDetallePedidos(){var n=[],e=this.localStorageService.getItem(F.DETALLE_PEDIDOS_ID);return e&&(n=e),(!n||n.length==0)&&(n=F.getDetallePedidos(),this.localStorageService.setItem(F.DETALLE_PEDIDOS_ID,n)),n}getMovimientosInventario(){var n=[],e=this.localStorageService.getItem(F.MOVIMIENTOS_INVENTARIO_ID);return e&&(n=e),(!n||n.length==0)&&(n=F.getMovimientosInventario(),this.localStorageService.setItem(F.MOVIMIENTOS_INVENTARIO_ID,n)),n}getPasarelas(){var n=[],e=this.localStorageService.getItem(F.PASARELA_ID);return e&&(n=e,this.localStorageService.setItem(F.PASARELA_ID,n)),(!n||n.length==0)&&(n=F.getPasarelas()),n}pushProducto(n){var e=this.getProductos();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(F.PRODUCTOS_ID,e)}pushLoteInventario(n){var e=this.getLotesInventario();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(F.LOTES_INVENTARIO_ID,e)}pushPedido(n){var e=this.getPedidos();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(F.PEDIDOS_ID,e)}pushDetallePedido(n){var e=this.getDetallePedidos();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(F.DETALLE_PEDIDOS_ID,e)}pushMovimientoInventario(n){var e=this.getMovimientosInventario();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(F.MOVIMIENTOS_INVENTARIO_ID,e)}editarProducto(n){var e=this.getProductos(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(F.PRODUCTOS_ID,i)}editarLoteInventario(n){var e=this.getLotesInventario(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(F.LOTES_INVENTARIO_ID,i)}editarDetallePedido(n){var e=this.getDetallePedidos(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(F.DETALLE_PEDIDOS_ID,i)}editarPedido(n){var e=this.getPedidos(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n),i.push(o)}),this.localStorageService.setItem(F.PEDIDOS_ID,i)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})};var zr=class t{formBuilder=d(Xh);validateFormControls(n){var e=n.valid,i=n.getRawValue();for(let r=0;r<i.length;r++)i[r]?.enabled&&!i[r]?.valid&&(e=!1);return e}getFormGroup(n){var e={};return n&&n.length>0&&n.forEach(i=>e[i.name]=[i.value||"",i.validators||[]]),this.formBuilder.group(e)}newProductoControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.descripcion,value:i})),[{id:1,name:"sku",label:"Codigo",type:"text",controlType:"input",validators:[Q.required,Q.minLength(3)]},{id:2,name:"nombre",label:"Nombre Producto",type:"text",controlType:"input",validators:[Q.required,Q.minLength(5)]},{id:3,name:"descripcion",label:"Descripcion",type:"text",controlType:"input",validators:[Q.required,Q.minLength(5)]},{id:4,name:"precio_venta",value:0,label:"Precio de Venta",type:"text",controlType:"input",validators:[Q.required,Q.min(.01)]},{id:5,name:"estado",label:"Estado",type:"text",controlType:"select",validators:[Q.required],options:e}]}newCompraFormControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre,value:i})),[{id:1,name:"producto",label:"Producto",type:"text",controlType:"select",validators:[Q.required],options:e},{id:2,name:"cantidad",value:0,label:"Cantidad",type:"number",controlType:"input",validators:[Q.required,Q.min(.01)]},{id:3,name:"pagoTotal",value:0,label:"Pago Total",type:"number",controlType:"input",validators:[Q.required,Q.min(.01)]}]}newVentaLineaFormControls(n){var e=[];return n&&n.length>0&&n.forEach(i=>e.push({id:i.id,name:i.nombre+"comision: $"+i.comision,value:i})),[{id:1,name:"pasarela",label:"Pasarela de pago",type:"text",controlType:"select",validators:[Q.required],options:e}]}newMermaForm(){return this.formBuilder.group({sku:["",[Q.required,Q.minLength(5)]],nombre:["",[Q.required,Q.minLength(3)]],descripcion:["",[Q.required,Q.minLength(3)]],precio_venta:[0,[Q.required,Q.min(.01)]],estado:[F.estadoProductoVacio(),[Q.required]]})}newDevolucionCompraForm(){return this.formBuilder.group({sku:["",[Q.required,Q.minLength(5)]],nombre:["",[Q.required,Q.minLength(3)]],descripcion:["",[Q.required,Q.minLength(3)]],precio_venta:[0,[Q.required,Q.min(.01)]],estado:[F.estadoProductoVacio(),[Q.required]]})}newDevolucionVentaForm(){return this.formBuilder.group({sku:["",[Q.required,Q.minLength(5)]],nombre:["",[Q.required,Q.minLength(3)]],descripcion:["",[Q.required,Q.minLength(3)]],precio_venta:[0,[Q.required,Q.min(.01)]],estado:[F.estadoProductoVacio(),[Q.required]]})}static \u0275fac=function(e){return new(e||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})};var $r=class{isErrorState(n,e){let i=e&&e.submitted;return!!(n&&n.invalid&&(n.dirty||n.touched||i))}};var kA=()=>["sku","nombre","descripcion","precio_venta","estado","opciones"];function sz(t,n){if(t&1){let e=Ct();v(0,"button",16),J("click",function(){Ye(e);let r=V();return Ze(r.crearProducto())}),M(1,"Crear Producto"),_()}}function az(t,n){if(t&1){let e=Ct();v(0,"button",16),J("click",function(){Ye(e);let r=V();return Ze(r.crearProducto())}),M(1,"Editar Producto"),_()}}function lz(t,n){t&1&&(v(0,"th",17),M(1,"Codigo"),_()),t&2&&Re("text-align","center")}function cz(t,n){if(t&1&&(v(0,"td",18),M(1),_()),t&2){let e=n.$implicit;Re("text-align","center"),D(),$e(" ",e.sku," ")}}function dz(t,n){t&1&&(v(0,"th",17),M(1,"Nombre"),_()),t&2&&Re("text-align","center")}function uz(t,n){if(t&1&&(v(0,"td",18),M(1),_()),t&2){let e=n.$implicit;Re("text-align","center"),D(),$e(" ",e.nombre)}}function fz(t,n){t&1&&(v(0,"th",17),M(1,"Descripcion"),_()),t&2&&Re("text-align","center")}function hz(t,n){if(t&1&&(v(0,"td",18),M(1),_()),t&2){let e=n.$implicit;Re("text-align","center"),D(),$e(" ",e.descripcion)}}function mz(t,n){t&1&&(v(0,"th",17),M(1,"Precio de Venta"),_()),t&2&&Re("text-align","center")}function pz(t,n){if(t&1&&(v(0,"td",18),M(1),Dr(2,"currency"),_()),t&2){let e=n.$implicit;Re("text-align","center"),D(),$e(" ",wr(2,3,e.precio_venta)," ")}}function gz(t,n){t&1&&(v(0,"th",17),M(1,"Periodo"),_()),t&2&&Re("text-align","center")}function vz(t,n){if(t&1&&(v(0,"td",18),M(1),_()),t&2){let e=n.$implicit;Re("text-align","center"),D(),$e(" ",e.estado.descripcion," ")}}function _z(t,n){t&1&&(v(0,"th",19),M(1,"OPCIONES"),_())}function yz(t,n){if(t&1){let e=Ct();v(0,"td",18)(1,"button",20),J("click",function(){let r=Ye(e).$implicit,o=V();return Ze(o.editarProducto(r))}),v(2,"mat-icon"),M(3,"edit"),_()()()}}function bz(t,n){t&1&&he(0,"tr",21)}function Dz(t,n){if(t&1){let e=Ct();v(0,"tr",22),J("click",function(){let r=Ye(e).$implicit,o=V();return Ze(o.seleccionarProducto(r))}),_()}}function wz(t,n){t&1&&(v(0,"label",0),M(1,"Agregar Producto"),_())}function Cz(t,n){t&1&&(v(0,"label",0),M(1,"Editar Producto"),_())}function Ez(t,n){if(t&1&&he(0,"textarea",4),t&2){let e=V().$implicit,i=V();re("formControlName",e.name)("errorStateMatcher",i.matcher)}}function xz(t,n){if(t&1&&(v(0,"mat-option",7),M(1),_()),t&2){let e=n.$implicit;re("value",e.value),D(),et(e.name)}}function Sz(t,n){if(t&1&&(v(0,"mat-select",5),Ni(1,xz,2,2,"mat-option",7,Oi),_()),t&2){let e=V().$implicit,i=V();re("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),D(),Pi(e.options)}}function Iz(t,n){if(t&1&&he(0,"input",6),t&2){let e=V().$implicit,i=V();re("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function Mz(t,n){if(t&1&&(v(0,"mat-hint"),M(1),_()),t&2){let e=V().$implicit;D(),et(e.label)}}function Tz(t,n){if(t&1&&(v(0,"mat-error"),M(1),_()),t&2){let e=V().$implicit;D(),$e("Seleccione ",e.label)}}function Az(t,n){if(t&1&&(v(0,"mat-error"),M(1),_()),t&2){let e=V().$implicit;D(),$e("Ingrese ",e.label)}}function Rz(t,n){if(t&1&&(v(0,"mat-card-title")(1,"mat-form-field")(2,"mat-label"),M(3),_(),z(4,Ez,1,2,"textarea",4)(5,Sz,3,3,"mat-select",5)(6,Iz,1,3,"input",6),z(7,Mz,2,1,"mat-hint"),z(8,Tz,2,1,"mat-error"),z(9,Az,2,1,"mat-error"),_()()),t&2){let e,i,r,o,s=n.$implicit,a=V();D(3),et(s.label),D(),$((e=s.controlType)==="textarea"?4:e==="select"?5:6),D(3),$((i=a.getFormControl(s.name))!=null&&i.errors?7:-1),D(),$((r=a.getFormControl(s.name))!=null&&r.errors&&s.controlType==="select"?8:-1),D(),$((o=a.getFormControl(s.name))!=null&&o.errors&&s.controlType!="select"?9:-1)}}function kz(t,n){t&1&&(v(0,"button",3),M(1,"Agregar"),_())}function Oz(t,n){t&1&&(v(0,"button",3),M(1,"Editar"),_())}var Rm=class t{innerWidths="0";document=d(P);dialog=d(Rc);dataService=d(un);productoSeleccionado=F.productoVacio();productosList=k([]);constructor(){this.innerWidths=this.document.body.clientWidth*.9+"px"}ngOnInit(){this.cargarListas()}cargarListas(){this.productosList.update(()=>[...this.dataService.getProductos()])}seleccionarProducto(n){this.productoSeleccionado=n}crearProducto(){let n=tt.getMatDialogConf();n.data={productoSeleccionado:this.productoSeleccionado},this.dialog.open(Xb,n).afterClosed().subscribe(i=>{this.cargarListas(),this.productoSeleccionado=F.productoVacio()})}editarProducto(n){this.productoSeleccionado=n,this.crearProducto()}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-productos"]],decls:31,vars:9,consts:[["multi",""],["hideToggle","","expanded","true"],["mat-stroked-button","","color","primary"],["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","sku"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","nombre"],["matColumnDef","descripcion"],["matColumnDef","precio_venta"],["matColumnDef","estado"],["matColumnDef","opciones","stickyEnd",""],["mat-header-cell","","aria-label","row actions",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",3,"click",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-cell","","aria-label","row actions"],["matMiniFab","",3,"click"],["mat-header-row",""],["mat-row","",3,"click"]],template:function(e,i){e&1&&(v(0,"mat-accordion",0)(1,"mat-expansion-panel",1)(2,"mat-expansion-panel-header")(3,"mat-panel-description"),z(4,sz,2,0,"button",2),z(5,az,2,0,"button",2),_()()(),v(6,"mat-expansion-panel",1)(7,"mat-expansion-panel-header")(8,"mat-panel-title"),M(9," Tabla Productos "),_()(),v(10,"table",3),It(11,4),Me(12,lz,2,2,"th",5)(13,cz,2,3,"td",6),Mt(),It(14,7),Me(15,dz,2,2,"th",5)(16,uz,2,3,"td",6),Mt(),It(17,8),Me(18,fz,2,2,"th",5)(19,hz,2,3,"td",6),Mt(),It(20,9),Me(21,mz,2,2,"th",5)(22,pz,3,5,"td",6),Mt(),It(23,10),Me(24,gz,2,2,"th",5)(25,vz,2,3,"td",6),Mt(),It(26,11),Me(27,_z,2,0,"th",12)(28,yz,4,0,"td",13),Mt(),Me(29,bz,1,0,"tr",14)(30,Dz,1,0,"tr",15),_()()()),e&2&&(D(4),$(!i.productoSeleccionado||i.productoSeleccionado.id==0?4:-1),D(),$(!i.productoSeleccionado||i.productoSeleccionado.id>0?5:-1),D(),Re("overflow","auto"),D(4),re("dataSource",i.productosList()),D(19),re("matHeaderRowDef",Co(7,kA)),D(),re("matRowDefColumns",Co(8,kA)))},dependencies:[ka,am,qo,Yo,Ea,sm,bi,iT,Sa,_m,bm,Em,Dm,ym,xm,wm,Cm,Sm,Im,Vs],encapsulation:2})},Xb=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px",this.seleccionarProducto(n.productoSeleccionado)}innerWidths="0";document=d(P);_snackBar=d(Hr);dataService=d(un);dialogRef=d(Ho);formConfigs=k([]);matcher=new $r;formService=d(zr);productoForm;productoSeleccionado=F.productoVacio();estadoProductoList=k([]);ngOnInit(){this.cargarListas();var n=this.formService.newProductoControls(this.estadoProductoList());this.productoForm=this.formService.getFormGroup(n),this.formConfigs.update(e=>[...n]),this.seleccionarProductoForm()}getFormControl(n){return this.productoForm.get(n)}compareIds(n,e){return n.id==e.id}seleccionarProducto(n){this.productoSeleccionado=n}seleccionarProductoForm(){this.getFormControl("id")?.setValue(this.productoSeleccionado.id),this.getFormControl("sku")?.setValue(this.productoSeleccionado.sku),this.getFormControl("nombre")?.setValue(this.productoSeleccionado.nombre),this.getFormControl("descripcion")?.setValue(this.productoSeleccionado.descripcion),this.getFormControl("precio_venta")?.setValue(this.productoSeleccionado.precio_venta),this.getFormControl("estado")?.setValue(this.productoSeleccionado.estado)}cargarListas(){this.estadoProductoList.update(n=>[...this.dataService.getEstadosProducto()])}guardarProducto(){if(this.validarDatos()){if(this.productoSeleccionado.id>0){var n=this.productoSeleccionado.id;this.productoSeleccionado=this.productoForm.value,this.productoSeleccionado.id=n,this.dataService.editarProducto(this.productoSeleccionado),tt.openSnackBar("CAMBIOS GUARDADOS EXITOSAMENTE","aceptar",this._snackBar)}else this.productoSeleccionado=this.productoForm.value,this.dataService.pushProducto(this.productoSeleccionado),tt.openSnackBar("NUEVO PRODUCTO CREADO EXITOSAMENTE","aceptar",this._snackBar);this.dialogRef.close()}else tt.openSnackBar("Datos incorrectos","ok",this._snackBar)}validarDatos(){var n=this.formService.validateFormControls(this.productoForm),e=parseFloat(this.getFormControl("precio_venta")?.value);return(!e||isNaN(e))&&(n=!1,this.getFormControl("precio_venta")?.setErrors({key:"1"})),n}static \u0275fac=function(e){return new(e||t)(ne(Ac))};static \u0275cmp=T({type:t,selectors:[["dialog-crear"]],decls:16,vars:5,consts:[[1,"h4"],["id","productoForm",3,"ngSubmit","formGroup"],["cols","1"],["mat-stroked-button","","color","primary","type","submit"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","errorStateMatcher"],[3,"value"]],template:function(e,i){e&1&&(v(0,"mat-dialog-content")(1,"div")(2,"mat-card")(3,"mat-card-title")(4,"label",0),M(5,"Formulario"),_(),z(6,wz,2,0,"label",0),z(7,Cz,2,0,"label",0),_(),v(8,"mat-card-content")(9,"form",1),J("ngSubmit",function(){return i.guardarProducto()}),v(10,"mat-grid-list",2),Ni(11,Rz,10,5,"mat-card-title",null,Oi),v(13,"mat-card-title"),z(14,kz,2,0,"button",3),z(15,Oz,2,0,"button",3),_()()()()()()()),e&2&&(D(6),$(!i.productoSeleccionado||i.productoSeleccionado.id==0?6:-1),D(),$(i.productoSeleccionado&&i.productoSeleccionado.id>0?7:-1),D(2),re("formGroup",i.productoForm),D(2),Pi(i.formConfigs()),D(3),$(!i.productoSeleccionado||i.productoSeleccionado.id==0?14:-1),D(),$(i.productoSeleccionado&&i.productoSeleccionado.id>0?15:-1))},dependencies:[Ra,bi,Nh,ba,wa,Da,Yi,Di,Vr,jr,xa,um,qi,Ia,ya,Nr,Pr,_a,Wo,Wn,Go],encapsulation:2})};var Ki=class t{dataService=d(un);localStorageService=d(Kt);constructor(){}getUsuarioAnonimo(){return{id:1,usuario:"anonim",password:"",nombres:"anonimo",apellidos:"",fecha_creacion:new Date}}getUsuarios(){return this.dataService.getUsuarios()}pushUsuario(n){var e=this.getUsuarios();n.id=e[e.length-1].id+1,e.push(n),this.localStorageService.setItem(F.USUARIOS_ID,e)}editarUsuario(n){var e=this.getUsuarios(),i=[];e.forEach(r=>{var o=r;o.id==n.id&&(o=n,tt.isSha256(o.password)||tt.generateSHA256(o.password).then(s=>o.password=s)),i.push(o)}),this.localStorageService.setItem(F.USUARIOS_ID,i)}validarUsuario(n,e){var i="";tt.generateSHA256(e).then(s=>i=s);var r=this.getUsuarios(),o=F.usuarioVacio();return r&&r.length&&r.filter(s=>s.usuario==n).filter(s=>s.password==i).forEach(s=>o=s),o&&o.id>1?o:null}getUsuarioLoggeado(){var n=F.usuarioVacio(),e=this.localStorageService.getItem("usuario");return e&&(n=e),n}static \u0275fac=function(e){return new(e||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})};function Nz(t,n){if(t&1&&(v(0,"label",1),M(1),_()),t&2){let e=V();D(),et(e.mensajeLogin)}}function Pz(t,n){t&1&&(v(0,"mat-error"),M(1,"Ingrese Usuario"),_())}function Fz(t,n){t&1&&(v(0,"mat-error"),M(1,"Ingrese Contrase\xF1a"),_())}var km=class t{constructor(n,e){this._router=n;this.formBuilder=e;this.loginForm=this.formBuilder.group({username:["",[Q.required,Q.minLength(3)]],password:["",[Q.required,Q.minLength(3)]]})}loginForm;mensajeLogin="";_snackBar=d(Hr);usuarioService=d(Ki);localStorage=d(Kt);matcher=new $r;login(){if(this.validarDatos()){var n=this.usuarioService.validarUsuario(this.loginF("username")?.value,this.loginF("password")?.value);if(n&&n.id>1){tt.openSnackBar("Login exitoso","ok",this._snackBar),this.localStorage.setItem(F.LOGGED_USUARIO,n),this.localStorage.setItem(F.LOGGED,"true");let e={queryParams:{logged:"true"}};this._router.navigate(["/menu/productos"],e)}else tt.openSnackBar("Credenciales incorrectas","ok",this._snackBar)}else tt.openSnackBar("Completar credenciales","ok",this._snackBar)}iniciarInvitado(){var n=this.usuarioService.getUsuarioAnonimo();this.localStorage.setItem(F.LOGGED_USUARIO,n),this.localStorage.setItem(F.LOGGED,"true");let e={queryParams:{logged:"true"}};this._router.navigate(["/menu/productos"],e)}validarDatos(){var n=this.loginForm.valid;return this.loginF("username")?.valid||(n=!1),this.loginF("password")?.valid||(n=!1),n}loginF(n){return this.loginForm.get(n)}static \u0275fac=function(e){return new(e||t)(ne(Un),ne(Xh))};static \u0275cmp=T({type:t,selectors:[["app-login"]],decls:28,vars:6,consts:[["hideToggle","","expanded","true",1,"login-container"],[1,"h4"],["id","loginForm",3,"ngSubmit","formGroup"],["cols","1"],["matInput","","required","","formControlName","username",3,"errorStateMatcher"],["matInput","","required","","formControlName","password","type","password",3,"errorStateMatcher"],["mat-stroked-button","","color","primary","type","submit"],["matButton","",3,"click"]],template:function(e,i){if(e&1&&(v(0,"mat-expansion-panel",0)(1,"mat-expansion-panel-header")(2,"mat-panel-title"),M(3," Iniciar Sesi\xF3n "),_()(),v(4,"mat-card")(5,"mat-card-title"),z(6,Nz,2,1,"label",1),_(),v(7,"mat-card-content")(8,"form",2),J("ngSubmit",function(){return i.login()}),v(9,"mat-grid-list",3)(10,"mat-card-title")(11,"mat-form-field")(12,"mat-label"),M(13,"Usuario"),_(),he(14,"input",4),z(15,Pz,2,0,"mat-error"),_()(),v(16,"mat-card-title")(17,"mat-form-field")(18,"mat-label"),M(19,"Contrase\xF1a"),_(),he(20,"input",5),z(21,Fz,2,0,"mat-error"),_()(),v(22,"mat-card-title")(23,"button",6),M(24,"Ingresar"),_()()()()()(),v(25,"mat-action-row")(26,"button",7),J("click",function(){return i.iniciarInvitado()}),M(27,"Continuar como invitado"),_()()()),e&2){let r,o;D(6),$(i.mensajeLogin&&i.mensajeLogin!=""?6:-1),D(2),re("formGroup",i.loginForm),D(6),re("errorStateMatcher",i.matcher),D(),$((r=i.loginF("username"))!=null&&r.errors?15:-1),D(5),re("errorStateMatcher",i.matcher),D(),$((o=i.loginF("password"))!=null&&o.errors?21:-1)}},dependencies:[Mm,qo,rA,Yo,Ea,bi,ba,wa,Da,Yi,Di,jr,xa,Ia,ya,Nr,Pr,_a,Wo,Wn,Go],encapsulation:2})};var Qo=class t{dataService=d(un);_snackBar=d(Hr);constructor(){}getPedidosUsuario(n){var e=[],i=this.dataService.getPedidos();if(i.length>0)for(let r of i)r.usuario.id==n.id&&r.estado.id<3&&e.push(r);return e}getUltimoPedidoUsuario(n){var e=F.pedidoVacio(),i=this.getPedidosUsuario(n);return i.length>0&&(e=i[i.length-1]),e}getDetallesPedido(n){var e=[],i=this.dataService.getDetallePedidos();if(i.length>0)for(let r of i)r.pedido.id==n.id&&e.push(r);return e}aumentarDetallePedido(n){var e=this.getDetallesPedido(n.pedido);for(let i of e)i.id==n.id&&(i.cantidad++,this.dataService.editarDetallePedido(i));return e}disminuirDetallePedido(n){var e=this.getDetallesPedido(n.pedido);for(let i of e)i.id==n.id&&i.cantidad>0&&(i.cantidad--,this.dataService.editarDetallePedido(i));return e}pedidoPagado(n){var e=this.dataService.getEstadosPedido();n.estado=e[2],this.dataService.editarPedido(n);var i=this.getDetallesPedido(n);for(let r of i)(r.pedido.id=n.id)&&(r.pedido=n,this.dataService.editarDetallePedido(r),this.registrarPagoInventario(r))}registrarPagoInventario(n){let e=F.movimientoInventarioVacio();e.cantidad=n.cantidad,e.fecha=new Date,e.pedido=n.pedido,e.producto=n.producto;let i=this.dataService.getTiposMovimiento();e.tipo_movimiento=i[i.findIndex(a=>a.id==2)];let r=this.dataService.getLotesInventario(),o=[],s=0;s=n.cantidad;for(let a of r)a.producto.id==n.producto.id&&a.cantidad_actual>0&&s>0&&(o.push(a),a.cantidad_actual<s&&(s-=a.cantidad_actual));s=n.cantidad;for(let a of o)if(s>0){a.cantidad_actual<s?(s-=a.cantidad_actual,a.cantidad_actual=0):(a.cantidad_actual-=s,s=0);let l=e;l.lote=a,this.dataService.editarLoteInventario(a),this.dataService.pushMovimientoInventario(l)}n.producto.stock_web&&n.producto.stock_web>0&&(n.producto.stock_web-=n.cantidad,this.dataService.editarProducto(n.producto))}validarExistenciaInventario(n){let e=this.dataService.getLotesInventario(),i=[],r=0;r=n.cantidad;for(let o of e)o.producto.id==n.producto.id&&o.cantidad_actual>0&&r>0&&(i.push(o),o.cantidad_actual<r?r-=o.cantidad_actual:r=0);return i&&i.length>0&&r==0}static \u0275fac=function(e){return new(e||t)};static \u0275prov=b({token:t,factory:t.\u0275fac,providedIn:"root"})};var OA=()=>["producto","cantidad","precio","opciones"];function Lz(t,n){t&1&&(v(0,"th",14),M(1,"Producto"),_()),t&2&&Re("text-align","center")}function Vz(t,n){if(t&1&&(v(0,"td",15),M(1),_()),t&2){let e=n.$implicit;Re("text-align","center"),D(),_f(" ",e.producto.sku," ",e.producto.nombre," ")}}function jz(t,n){t&1&&(v(0,"th",14),M(1,"Cantidad"),_()),t&2&&Re("text-align","center")}function Bz(t,n){if(t&1&&(v(0,"td",15),M(1),_()),t&2){let e=n.$implicit;Re("text-align","center"),D(),$e(" ",e.cantidad)}}function Uz(t,n){t&1&&(v(0,"th",14),M(1,"Precio"),_()),t&2&&Re("text-align","center")}function Hz(t,n){if(t&1&&(v(0,"td",15),M(1),Dr(2,"currency"),_()),t&2){let e=n.$implicit;Re("text-align","center"),D(),$e(" ",wr(2,3,e.precio_unitario_venta))}}function zz(t,n){t&1&&(v(0,"th",16),M(1,"OPCIONES"),_())}function $z(t,n){if(t&1){let e=Ct();v(0,"td",15)(1,"button",17),J("click",function(){let r=Ye(e).$implicit,o=V();return Ze(o.aumentarDetallePedido(r))}),v(2,"mat-icon"),M(3,"arrow_circle_up"),_()(),v(4,"button",17),J("click",function(){let r=Ye(e).$implicit,o=V();return Ze(o.disminuirDetallePedido(r))}),v(5,"mat-icon"),M(6,"arrow_circle_down"),_()()()}}function Gz(t,n){t&1&&he(0,"tr",18)}function Wz(t,n){t&1&&he(0,"tr",19)}function qz(t,n){if(t&1&&he(0,"textarea",4),t&2){let e=V().$implicit,i=V();re("formControlName",e.name)("errorStateMatcher",i.matcher)}}function Yz(t,n){if(t&1&&(v(0,"mat-option",8),M(1),_()),t&2){let e=n.$implicit;re("value",e.value),D(),et(e.name)}}function Zz(t,n){if(t&1){let e=Ct();v(0,"mat-select",7),J("selectionChange",function(r){Ye(e);let o=V(2);return Ze(o.onSelectionChange(r))}),Ni(1,Yz,2,2,"mat-option",8,Oi),_()}if(t&2){let e=V().$implicit,i=V();re("compareWith",i.compareIds)("formControlName",e.name)("errorStateMatcher",i.matcher),D(),Pi(e.options)}}function Kz(t,n){if(t&1&&he(0,"input",6),t&2){let e=V().$implicit,i=V();re("type",e.type)("formControlName",e.name)("errorStateMatcher",i.matcher)}}function Qz(t,n){if(t&1&&(v(0,"mat-hint"),M(1),_()),t&2){let e=V().$implicit;D(),et(e.label)}}function Xz(t,n){if(t&1&&(v(0,"mat-error"),M(1),_()),t&2){let e=V().$implicit;D(),$e("Seleccione ",e.label)}}function Jz(t,n){if(t&1&&(v(0,"mat-error"),M(1),_()),t&2){let e=V().$implicit;D(),$e("Ingrese ",e.label)}}function e$(t,n){if(t&1&&(v(0,"div")(1,"mat-form-field")(2,"mat-label"),M(3),_(),z(4,qz,1,2,"textarea",4)(5,Zz,3,3,"mat-select",5)(6,Kz,1,3,"input",6),z(7,Qz,2,1,"mat-hint"),z(8,Xz,2,1,"mat-error"),z(9,Jz,2,1,"mat-error"),_()()),t&2){let e,i,r,o,s=n.$implicit,a=V();D(3),et(s.label),D(),$((e=s.controlType)==="textarea"?4:e==="select"?5:6),D(3),$((i=a.getFormControl(s.name))!=null&&i.errors?7:-1),D(),$((r=a.getFormControl(s.name))!=null&&r.errors&&s.controlType==="select"?8:-1),D(),$((o=a.getFormControl(s.name))!=null&&o.errors&&s.controlType!="select"?9:-1)}}var Om=class t{innerWidths="0";document=d(P);dialog=d(Rc);usuarioService=d(Ki);carritoService=d(Qo);usuario=k(F.usuarioVacio());pedido=k(F.pedidoVacio());detallePedidoList=k([]);constructor(){}ngOnInit(){this.innerWidths=this.document.body.clientWidth*.9+"px",this.usuario.update(n=>this.usuarioService.getUsuarioLoggeado()),this.cargarListas()}cargarListas(){this.pedido.update(n=>this.carritoService.getUltimoPedidoUsuario(this.usuario())),this.detallePedidoList.update(n=>[...this.carritoService.getDetallesPedido(this.pedido())])}aumentarDetallePedido(n){this.detallePedidoList.update(e=>[...this.carritoService.aumentarDetallePedido(n)])}disminuirDetallePedido(n){this.detallePedidoList.update(e=>[...this.carritoService.disminuirDetallePedido(n)])}pagoCarrito(){let n=tt.getMatDialogConf();n.data={detallePedidoList:this.detallePedidoList()},this.dialog.open(Jb,n).afterClosed().subscribe(i=>{this.cargarListas()})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-carrito"]],decls:25,vars:7,consts:[["multi",""],["hideToggle","","expanded","true"],["mat-table","",1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","producto"],["mat-header-cell","",3,"textAlign",4,"matHeaderCellDef"],["mat-cell","",3,"textAlign",4,"matCellDef"],["matColumnDef","cantidad"],["matColumnDef","precio"],["matColumnDef","opciones","stickyEnd",""],["mat-header-cell","","aria-label","row actions",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-stroked-button","","color","primary",3,"click"],["mat-header-cell",""],["mat-cell",""],["mat-header-cell","","aria-label","row actions"],["matIconButton","",3,"click"],["mat-header-row",""],["mat-row",""]],template:function(e,i){e&1&&(v(0,"mat-accordion",0)(1,"mat-expansion-panel",1)(2,"mat-expansion-panel-header")(3,"mat-panel-title"),M(4," Carrito de compras "),_()(),v(5,"table",2),It(6,3),Me(7,Lz,2,2,"th",4)(8,Vz,2,4,"td",5),Mt(),It(9,6),Me(10,jz,2,2,"th",4)(11,Bz,2,3,"td",5),Mt(),It(12,7),Me(13,Uz,2,2,"th",4)(14,Hz,3,5,"td",5),Mt(),It(15,8),Me(16,zz,2,0,"th",9)(17,$z,7,0,"td",10),Mt(),Me(18,Gz,1,0,"tr",11)(19,Wz,1,0,"tr",12),_()(),v(20,"mat-expansion-panel",1)(21,"mat-expansion-panel-header")(22,"mat-panel-description")(23,"button",13),J("click",function(){return i.pagoCarrito()}),M(24,"Pagar Carrito"),_()()()()()),e&2&&(D(),Re("overflow","auto"),D(4),re("dataSource",i.detallePedidoList()),D(13),re("matHeaderRowDef",Co(5,OA)),D(),re("matRowDefColumns",Co(6,OA)))},dependencies:[ka,am,qo,Yo,Ea,sm,bi,Nc,Sa,_m,bm,Em,Dm,ym,xm,wm,Cm,Sm,Im,Vs],encapsulation:2})},Jb=class t{constructor(n){this.data=n;this.innerWidths=this.document.body.clientWidth*.9+"px",this.cargarDatos(n.detallePedidoList)}innerWidths="0";document=d(P);_snackBar=d(Hr);dialogRef=d(Ho);formConfigs=k([]);matcher=new $r;formService=d(zr);dataService=d(un);carritoService=d(Qo);usuarioService=d(Ki);pagoCarritoForm;detallePedidoList=k([]);pasarelaList=k([]);totalPedido=k(0);comisionPasarela=k(0);totalPago=k(0);ngOnInit(){this.cargarListas();var n=this.formService.newVentaLineaFormControls(this.pasarelaList());this.pagoCarritoForm=this.formService.getFormGroup(n),this.formConfigs.update(e=>[...n]),this.cargarDatosForm()}getFormControl(n){return this.pagoCarritoForm.get(n)}compareIds(n,e){return n.id==e.id}cargarDatos(n){var e=this.usuarioService.getUsuarioLoggeado(),i=this.carritoService.getUltimoPedidoUsuario(e);this.detallePedidoList.update(r=>[...this.carritoService.getDetallesPedido(i)])}cargarDatosForm(){if(this.detallePedidoList()&&this.detallePedidoList().length>0){let n=0;for(let e of this.detallePedidoList())n+=e.precio_unitario_venta*e.cantidad;this.totalPedido.update(e=>n),this.totalPago.update(e=>n)}}cargarListas(){this.pasarelaList.update(n=>[...this.dataService.getPasarelas()])}onSelectionChange(n){let e=n.value;this.comisionPasarela.update(i=>e.comision),this.totalPago.update(i=>this.totalPedido()+e.comision)}pagarPedido(){if(this.validarDatos())if(this.totalPago()>0&&this.totalPedido()>0){var n=this.detallePedidoList()[0].pedido;this.carritoService.pedidoPagado(n),tt.openSnackBar("Pago exitoso","aceptar",this._snackBar),this.dialogRef.close()}else tt.openSnackBar("Datos Incorrectos","aceptar",this._snackBar);else tt.openSnackBar("Datos incorrectos","ok",this._snackBar)}validarDatos(){var n=this.formService.validateFormControls(this.pagoCarritoForm);return n}static \u0275fac=function(e){return new(e||t)(ne(Ac))};static \u0275cmp=T({type:t,selectors:[["dialog-crear"]],decls:32,vars:10,consts:[[1,"h4"],["id","productoForm",3,"ngSubmit","formGroup"],["cols","1"],["mat-stroked-button","","color","primary","type","submit"],["matInput","",3,"formControlName","errorStateMatcher"],["required","",3,"compareWith","formControlName","errorStateMatcher"],["matInput","","required","",3,"type","formControlName","errorStateMatcher"],["required","",3,"selectionChange","compareWith","formControlName","errorStateMatcher"],[3,"value"]],template:function(e,i){e&1&&(v(0,"mat-dialog-content")(1,"div")(2,"mat-card")(3,"mat-card-title")(4,"label",0),M(5,"Pagar Pedido"),_()(),v(6,"mat-card-content")(7,"form",1),J("ngSubmit",function(){return i.pagarPedido()}),v(8,"mat-grid-list",2)(9,"div")(10,"mat-label"),M(11,"Total de pedido: "),_(),v(12,"mat-label"),M(13),Dr(14,"currency"),_()(),Ni(15,e$,10,5,"div",null,Oi),v(17,"div")(18,"mat-label"),M(19,"comision pasarela: "),_(),v(20,"mat-label"),M(21),Dr(22,"currency"),_()(),v(23,"div")(24,"mat-label"),M(25,"Total a pagar: "),_(),v(26,"mat-label"),M(27),Dr(28,"currency"),_()(),v(29,"div")(30,"button",3),M(31,"Pagar"),_()()()()()()()()),e&2&&(D(7),re("formGroup",i.pagoCarritoForm),D(6),et(wr(14,4,i.totalPedido())),D(2),Pi(i.formConfigs()),D(6),et(wr(22,6,i.comisionPasarela())),D(6),et(wr(28,8,i.totalPago())))},dependencies:[Ra,bi,Nh,ba,wa,Da,Yi,Di,Vr,jr,xa,um,qi,Ia,ya,Nr,Pr,_a,Wo,Wn,Go,Vh,Vs],encapsulation:2})};var NA=[{path:"",redirectTo:"/menu",pathMatch:"full"},{path:"menu",component:Am,children:[{path:"",redirectTo:"/menu/login",pathMatch:"full"},{path:"login",component:km},{path:"productos",component:Rm},{path:"carrito",component:Om}]}];var t$=[un,zr,Kt,Ki,Oa,Qo],PA={providers:[t$,{provide:Zy,useValue:{hasBackdrop:!1}},og(),Iy(NA,My()),eI(JS())]};var Nm=class t{title=k("techstoresv");static \u0275fac=function(e){return new(e||t)};static \u0275cmp=T({type:t,selectors:[["app-root"]],decls:1,vars:0,template:function(e,i){e&1&&he(0,"router-outlet")},dependencies:[ko],styles:['[_nghost-%COMP%]{--bright-blue: oklch(51.01% .274 263.83);--electric-violet: oklch(53.18% .28 296.97);--french-violet: oklch(47.66% .246 305.88);--vivid-pink: oklch(69.02% .277 332.77);--hot-red: oklch(61.42% .238 15.34);--orange-red: oklch(63.32% .24 31.68);--gray-900: oklch(19.37% .006 300.98);--gray-700: oklch(36.98% .014 302.71);--gray-400: oklch(70.9% .015 304.04);--red-to-pink-to-purple-vertical-gradient: linear-gradient( 180deg, var(--orange-red) 0%, var(--vivid-pink) 50%, var(--electric-violet) 100% );--red-to-pink-to-purple-horizontal-gradient: linear-gradient( 90deg, var(--orange-red) 0%, var(--vivid-pink) 50%, var(--electric-violet) 100% );--pill-accent: var(--bright-blue);font-family:Inter,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif,"Apple Color Emoji","Segoe UI Emoji",Segoe UI Symbol;box-sizing:border-box;-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale;display:block;height:100dvh}h1[_ngcontent-%COMP%]{font-size:3.125rem;color:var(--gray-900);font-weight:500;line-height:100%;letter-spacing:-.125rem;margin:0;font-family:Inter Tight,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif,"Apple Color Emoji","Segoe UI Emoji",Segoe UI Symbol}p[_ngcontent-%COMP%]{margin:0;color:var(--gray-700)}main[_ngcontent-%COMP%]{width:100%;min-height:100%;display:flex;justify-content:center;align-items:center;padding:1rem;box-sizing:inherit;position:relative}.angular-logo[_ngcontent-%COMP%]{max-width:9.2rem}.content[_ngcontent-%COMP%]{display:flex;justify-content:space-around;width:100%;max-width:700px;margin-bottom:3rem}.content[_ngcontent-%COMP%]   h1[_ngcontent-%COMP%]{margin-top:1.75rem}.content[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin-top:1.5rem}.divider[_ngcontent-%COMP%]{width:1px;background:var(--red-to-pink-to-purple-vertical-gradient);margin-inline:.5rem}.pill-group[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:start;flex-wrap:wrap;gap:1.25rem}.pill[_ngcontent-%COMP%]{display:flex;align-items:center;--pill-accent: var(--bright-blue);background:color-mix(in srgb,var(--pill-accent) 5%,transparent);color:var(--pill-accent);padding-inline:.75rem;padding-block:.375rem;border-radius:2.75rem;border:0;transition:background .3s ease;font-family:var(--inter-font);font-size:.875rem;font-style:normal;font-weight:500;line-height:1.4rem;letter-spacing:-.00875rem;text-decoration:none;white-space:nowrap}.pill[_ngcontent-%COMP%]:hover{background:color-mix(in srgb,var(--pill-accent) 15%,transparent)}.pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+1){--pill-accent: var(--bright-blue)}.pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+2){--pill-accent: var(--electric-violet)}.pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+3){--pill-accent: var(--french-violet)}.pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+4), .pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+5), .pill-group[_ngcontent-%COMP%]   .pill[_ngcontent-%COMP%]:nth-child(6n+6){--pill-accent: var(--hot-red)}.pill-group[_ngcontent-%COMP%]   svg[_ngcontent-%COMP%]{margin-inline-start:.25rem}.social-links[_ngcontent-%COMP%]{display:flex;align-items:center;gap:.73rem;margin-top:1.5rem}.social-links[_ngcontent-%COMP%]   path[_ngcontent-%COMP%]{transition:fill .3s ease;fill:var(--gray-400)}.social-links[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]:hover   svg[_ngcontent-%COMP%]   path[_ngcontent-%COMP%]{fill:var(--gray-900)}@media screen and (max-width:650px){.content[_ngcontent-%COMP%]{flex-direction:column;width:max-content}.divider[_ngcontent-%COMP%]{height:1px;width:100%;background:var(--red-to-pink-to-purple-horizontal-gradient);margin-block:1.5rem}}']})};$_(Nm,PA).catch(t=>console.error(t));
