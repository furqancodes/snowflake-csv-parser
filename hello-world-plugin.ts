// Hello World Plugin
console.log("Hello World - This is a plugin");

export default function helloWorldPlugin() {
  return {
    name: "hello-world-plugin",
    version: "1.0.0",
    execute: () => {
      console.log("Hello World - This is a plugin");
    }
  };
}
