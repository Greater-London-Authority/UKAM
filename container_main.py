#!/usr/bin/env python3
"""
AWS ECS Container Entry Point for UK Address Matcher
Routes to canonical or messy processing based on PROCESSING_MODE
"""

import os
import sys


def main():
    """Container entry point - routes to appropriate processor"""
    processing_mode = os.environ.get('PROCESSING_MODE', 'messy').lower()

    print("   UK Address Matcher Container Starting...")
    print(f"   Processing Mode: {processing_mode}")

    if processing_mode == 'canonical':
        print("   Starting CANONICAL data processing...")
        from process_canonical import CanonicalProcessor
        processor = CanonicalProcessor()
        processor.process()

    elif processing_mode == 'messy':
        print("   Starting MESSY data matching...")
        from process_messy import MessyProcessor
        processor = MessyProcessor()
        processor.process()

    else:
        print(f"   Unknown processing mode: {processing_mode}")
        print("   Valid options: 'canonical' or 'messy'")
        sys.exit(1)


if __name__ == "__main__":
    main()
