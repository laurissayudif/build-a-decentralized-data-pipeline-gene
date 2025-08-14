const IPFS = require('ipfs-api');
const { createHash } = require('crypto');
const fs = require('fs');
const NodeRSA = require('node-rsa');

class DecentralizedDataPipelineGenerator {
  constructor(ipfsNode, rsaKey) {
    this.ipfs = IPFS(ipfsNode);
    this.rsaKey = rsaKey;
  }

  async generatePipeline(dataSources, dataTransformations, dataSinks) {
    const pipelineDefinition = {
      dataSources,
      dataTransformations,
      dataSinks
    };

    const pipelineHash = createHash('sha256');
    pipelineHash.update(JSON.stringify(pipelineDefinition));
    const pipelineId = pipelineHash.digest('hex');

    const pipelineConfig = await this.createPipelineConfig(pipelineDefinition);
    const pipelineConfigHash = await this.publishConfigToIPFS(pipelineConfig);
    const encryptedConfigHash = await this.encryptConfigHash(pipelineConfigHash);

    return {
      pipelineId,
      pipelineConfigHash,
      encryptedConfigHash
    };
  }

  async createPipelineConfig(pipelineDefinition) {
    const configData = JSON.stringify(pipelineDefinition);
    const configFile = `pipeline-${Date.now()}.json`;
    fs.writeFileSync(configFile, configData);
    return configFile;
  }

  async publishConfigToIPFS(configFile) {
    const fileBuffer = fs.readFileSync(configFile);
    const ipfsResult = await this.ipfs.add(fileBuffer);
    return ipfsResult[0].hash;
  }

  async encryptConfigHash(configHash) {
    const encryptedHash = this.rsaKey.encrypt(configHash, 'base64');
    return encryptedHash;
  }
}

module.exports = DecentralizedDataPipelineGenerator;