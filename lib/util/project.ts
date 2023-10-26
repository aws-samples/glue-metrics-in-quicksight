// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0.

import * as path from "path";

// Common Project relative paths for use in CDK constructs
export const PROJECT_ROOT = path.resolve(__dirname, "..");
export const LAMBDA_DIR = path.resolve(__dirname, "..", "lambda");
export const TEMPLATE_DIR = path.resolve(__dirname, "..", "templates");